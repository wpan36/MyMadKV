#include "replicated_kv_service.h"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <stdexcept>
#include <utility>
#include <vector>

#include "partitioning.h"

namespace madkv::kvraft {
namespace {

namespace kv = kvstore_service;
namespace storage = madkv::storage;
namespace raftcore = madkv::raftcore;
namespace partitioning = madkv::partitioning;

class WrongPartitionError final : public std::runtime_error {
public:
    WrongPartitionError(
        const std::uint32_t owner,
        const std::uint32_t actual
    )
        : std::runtime_error(
              "WRONG_PARTITION owner=" +
              std::to_string(owner) +
              " actual=" +
              std::to_string(actual)
          ) {
    }
};

}  // namespace

ReplicatedKVService::ReplicatedKVService(
    InMemoryKVStore& store,
    raftcore::RaftNode& raft,
    const std::uint32_t partition_id,
    const std::uint32_t partition_count
)
    : store_(store),
      raft_(raft),
      partition_id_(partition_id),
      partition_count_(partition_count) {
    if (partition_count_ == 0) {
        throw std::invalid_argument(
            "partition_count must be positive"
        );
    }

    if (partition_id_ >= partition_count_) {
        throw std::invalid_argument(
            "partition_id is outside partition_count"
        );
    }
}

bool ReplicatedKVService::SameReply(
    const MutationReply& left,
    const MutationReply& right
) {
    return
        left.found == right.found &&
        left.value == right.value;
}

std::string ReplicatedKVService::RequestKey(
    const std::string& client_id,
    const std::uint64_t request_id
) {
    return client_id + ":" + std::to_string(request_id);
}

std::optional<ReplicatedKVService::MutationReply>
ReplicatedKVService::DedupTable::Lookup(
    const Command& command
) const {
    const auto iterator = records_.find(
        ReplicatedKVService::RequestKey(
            command.client_id(),
            command.request_id()
        )
    );

    if (iterator == records_.end()) {
        return std::nullopt;
    }

    const DedupRecord& record = iterator->second;

    if (
        record.type != command.type() ||
        record.key != command.key() ||
        record.value != command.value()
    ) {
        throw std::invalid_argument(
            "client_id/request_id was reused for a "
            "different mutation"
        );
    }

    return record.reply;
}

void ReplicatedKVService::DedupTable::Remember(
    const Command& command,
    const MutationReply& reply,
    const std::uint64_t raft_index
) {
    const std::string identity =
        ReplicatedKVService::RequestKey(
            command.client_id(),
            command.request_id()
        );

    DedupRecord record;
    record.type = command.type();
    record.key = command.key();
    record.value = command.value();
    record.reply = reply;
    record.raft_index = raft_index;

    const auto [iterator, inserted] =
        records_.emplace(identity, record);

    if (!inserted) {
        const DedupRecord& existing = iterator->second;

        if (
            existing.type != record.type ||
            existing.key != record.key ||
            existing.value != record.value ||
            !ReplicatedKVService::SameReply(
                existing.reply,
                record.reply
            )
        ) {
            throw std::runtime_error(
                "Raft log contains conflicting records "
                "for one request identity"
            );
        }
    }
}

void ReplicatedKVService::ValidateRequestHeader(
    const kv::RequestHeader& header
) {
    if (header.client_id().empty()) {
        throw std::invalid_argument(
            "mutation request has an empty client_id"
        );
    }

    if (header.request_id() == 0) {
        throw std::invalid_argument(
            "mutation request_id must be greater than zero"
        );
    }
}

ReplicatedKVService::Command
ReplicatedKVService::MakeMutationCommand(
    const storage::MutationType type,
    const std::string& key,
    const std::string& value,
    const kv::RequestHeader& header
) {
    ValidateRequestHeader(header);

    if (
        type != storage::MUTATION_TYPE_PUT &&
        type != storage::MUTATION_TYPE_SWAP &&
        type != storage::MUTATION_TYPE_DELETE
    ) {
        throw std::invalid_argument(
            "invalid mutation type for client command"
        );
    }

    Command command;
    command.set_type(type);
    command.set_key(key);
    command.set_value(value);
    command.set_client_id(header.client_id());
    command.set_request_id(header.request_id());

    return command;
}

std::string ReplicatedKVService::SerializeCommand(
    const Command& command
) {
    std::string serialized;

    if (!command.SerializeToString(&serialized)) {
        throw std::runtime_error(
            "failed to serialize KV state-machine command"
        );
    }

    return serialized;
}

grpc::Status ReplicatedKVService::StatusFromProposeResult(
    const raftcore::ProposeResult& result
) {
    using raftcore::ProposeStatus;

    const std::string leader =
        result.leader_id.has_value()
            ? std::to_string(*result.leader_id)
            : "unknown";

    switch (result.status) {
        case ProposeStatus::Committed:
            return grpc::Status::OK;

        case ProposeStatus::NotLeader:
            return grpc::Status(
                grpc::StatusCode::FAILED_PRECONDITION,
                "NOT_LEADER leader_id=" + leader
            );

        case ProposeStatus::TimedOut:
            return grpc::Status(
                grpc::StatusCode::DEADLINE_EXCEEDED,
                "RAFT_COMMIT_TIMEOUT leader_id=" + leader
            );

        case ProposeStatus::Stopped:
            return grpc::Status(
                grpc::StatusCode::UNAVAILABLE,
                "RAFT_STOPPED"
            );
    }

    return grpc::Status(
        grpc::StatusCode::INTERNAL,
        "unknown Raft propose status"
    );
}

grpc::Status ReplicatedKVService::ErrorStatus(
    const std::exception& error
) {
    if (
        dynamic_cast<const WrongPartitionError*>(
            &error
        ) != nullptr
    ) {
        return grpc::Status(
            grpc::StatusCode::FAILED_PRECONDITION,
            error.what()
        );
    }

    if (
        dynamic_cast<const std::invalid_argument*>(
            &error
        ) != nullptr
    ) {
        return grpc::Status(
            grpc::StatusCode::INVALID_ARGUMENT,
            error.what()
        );
    }

    return grpc::Status(
        grpc::StatusCode::INTERNAL,
        error.what()
    );
}

void ReplicatedKVService::EnsureOwnsKey(
    const std::string& key
) const {
    const std::uint32_t owner =
        partitioning::OwnerForKey(
            key,
            partition_count_
        );

    if (owner != partition_id_) {
        throw WrongPartitionError(
            owner,
            partition_id_
        );
    }
}

ReplicatedKVService::MutationReply
ReplicatedKVService::ApplyMutationLocked(
    const Command& command
) {
    switch (command.type()) {
        case storage::MUTATION_TYPE_PUT:
            return MutationReply{
                store_.Put(
                    command.key(),
                    command.value()
                ),
                ""
            };

        case storage::MUTATION_TYPE_SWAP: {
            const auto old_value = store_.Swap(
                command.key(),
                command.value()
            );

            return old_value.has_value()
                ? MutationReply{true, *old_value}
                : MutationReply{false, ""};
        }

        case storage::MUTATION_TYPE_DELETE:
            return MutationReply{
                store_.Delete(command.key()),
                ""
            };

        case storage::MUTATION_TYPE_NOOP:
        case storage::MUTATION_TYPE_UNSPECIFIED:
            throw std::runtime_error(
                "non-mutation command reached ApplyMutationLocked"
            );
    }

    throw std::runtime_error(
        "unknown KV state-machine mutation type"
    );
}

void ReplicatedKVService::ApplyOneCommittedEntryLocked(
    const raftcore::RaftStorage::Entry& entry
) {
    Command command;

    if (!command.ParseFromString(entry.command())) {
        throw std::runtime_error(
            "failed to deserialize committed KV command "
            "at Raft index " +
            std::to_string(entry.index())
        );
    }

    command.set_sequence(entry.index());

    if (command.type() == storage::MUTATION_TYPE_NOOP) {
        return;
    }

    if (
        command.type() != storage::MUTATION_TYPE_PUT &&
        command.type() != storage::MUTATION_TYPE_SWAP &&
        command.type() != storage::MUTATION_TYPE_DELETE
    ) {
        throw std::runtime_error(
            "committed Raft entry contains invalid KV command type"
        );
    }

    if (
        command.client_id().empty() ||
        command.request_id() == 0
    ) {
        throw std::runtime_error(
            "committed mutation is missing request identity"
        );
    }

    EnsureOwnsKey(command.key());

    const auto cached = dedup_table_.Lookup(command);

    if (cached.has_value()) {
        // A retry may appear at more than one committed Raft index after
        // leader failure. Only the first occurrence mutates the state.
        return;
    }

    const MutationReply reply =
        ApplyMutationLocked(command);

    dedup_table_.Remember(
        command,
        reply,
        entry.index()
    );
}

void ReplicatedKVService::ApplyCommittedThrough(
    const std::uint64_t target_index
) {
    std::lock_guard<std::mutex> lock(state_mutex_);

    while (last_applied_ < target_index) {
        const std::uint64_t remaining =
            target_index - last_applied_;

        const std::size_t batch_size =
            static_cast<std::size_t>(
                std::min<std::uint64_t>(
                    remaining,
                    128
                )
            );

        const auto entries =
            raft_.GetCommittedEntries(
                last_applied_ + 1,
                batch_size
            );

        if (entries.empty()) {
            throw std::runtime_error(
                "committed Raft prefix has a gap before index " +
                std::to_string(target_index)
            );
        }

        for (const auto& entry : entries) {
            if (entry.index() != last_applied_ + 1) {
                throw std::runtime_error(
                    "committed Raft entries are not contiguous"
                );
            }

            if (entry.index() > target_index) {
                break;
            }

            ApplyOneCommittedEntryLocked(entry);
            last_applied_ = entry.index();
        }
    }
}

void ReplicatedKVService::RecoverCommittedState() {
    const std::uint64_t commit_index =
        raft_.CommitIndex();

    ApplyCommittedThrough(commit_index);

    std::fprintf(
        stderr,
        "KV state machine recovered through Raft index %llu\n",
        static_cast<unsigned long long>(last_applied_)
    );
}

std::uint64_t ReplicatedKVService::LastApplied() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return last_applied_;
}

grpc::Status ReplicatedKVService::ExecuteMutation(
    Command command,
    MutationReply* reply
) {
    try {
        EnsureOwnsKey(command.key());

        // Followers do not serve mutation RPCs.
        if (!raft_.IsLeader()) {
            raftcore::ProposeResult result;
            result.status = raftcore::ProposeStatus::NotLeader;
            result.term = raft_.CurrentTerm();
            result.leader_id = raft_.LeaderId();
            return StatusFromProposeResult(result);
        }

        // Bring the volatile state machine to the currently committed prefix
        // before checking the request-dedup table.
        ApplyCommittedThrough(raft_.CommitIndex());

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            const auto cached = dedup_table_.Lookup(command);

            if (cached.has_value()) {
                *reply = *cached;
                return grpc::Status::OK;
            }
        }

        const raftcore::ProposeResult result =
            raft_.Propose(
                SerializeCommand(command),
                kProposeTimeout
            );

        if (!result.committed()) {
            return StatusFromProposeResult(result);
        }

        // A committed Raft entry is not necessarily applied yet.
        ApplyCommittedThrough(result.index);

        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            const auto cached = dedup_table_.Lookup(command);

            if (!cached.has_value()) {
                throw std::runtime_error(
                    "committed mutation was not applied "
                    "to the KV state machine"
                );
            }

            *reply = *cached;
        }

        return grpc::Status::OK;
    } catch (const std::exception& error) {
        return ErrorStatus(error);
    }
}

grpc::Status ReplicatedKVService::ExecuteReadBarrier() {
    try {
        Command barrier;
        barrier.set_type(storage::MUTATION_TYPE_NOOP);

        const raftcore::ProposeResult result =
            raft_.Propose(
                SerializeCommand(barrier),
                kProposeTimeout
            );

        if (!result.committed()) {
            return StatusFromProposeResult(result);
        }

        // This read linearizes at the committed no-op. Applying through that
        // index includes every state-machine command ordered before it.
        ApplyCommittedThrough(result.index);

        return grpc::Status::OK;
    } catch (const std::exception& error) {
        return ErrorStatus(error);
    }
}

grpc::Status ReplicatedKVService::Put(
    grpc::ServerContext* /*context*/,
    const kv::PutRequest* request,
    kv::PutReply* reply
) {
    try {
        MutationReply result;
        const grpc::Status status =
            ExecuteMutation(
                MakeMutationCommand(
                    storage::MUTATION_TYPE_PUT,
                    request->key(),
                    request->new_value(),
                    request->header()
                ),
                &result
            );

        if (status.ok()) {
            reply->set_found(result.found);
        }

        return status;
    } catch (const std::exception& error) {
        return ErrorStatus(error);
    }
}

grpc::Status ReplicatedKVService::Swap(
    grpc::ServerContext* /*context*/,
    const kv::SwapRequest* request,
    kv::SwapReply* reply
) {
    try {
        MutationReply result;
        const grpc::Status status =
            ExecuteMutation(
                MakeMutationCommand(
                    storage::MUTATION_TYPE_SWAP,
                    request->key(),
                    request->new_value(),
                    request->header()
                ),
                &result
            );

        if (status.ok()) {
            reply->set_found(result.found);

            if (result.found) {
                reply->set_old_value(result.value);
            } else {
                reply->clear_old_value();
            }
        }

        return status;
    } catch (const std::exception& error) {
        return ErrorStatus(error);
    }
}

grpc::Status ReplicatedKVService::Get(
    grpc::ServerContext* /*context*/,
    const kv::GetRequest* request,
    kv::GetReply* reply
) {
    try {
        EnsureOwnsKey(request->key());

        const grpc::Status barrier =
            ExecuteReadBarrier();

        if (!barrier.ok()) {
            return barrier;
        }

        std::lock_guard<std::mutex> lock(state_mutex_);
        const auto value = store_.Get(request->key());

        reply->set_found(value.has_value());

        if (value.has_value()) {
            reply->set_value(*value);
        } else {
            reply->clear_value();
        }

        return grpc::Status::OK;
    } catch (const std::exception& error) {
        return ErrorStatus(error);
    }
}

grpc::Status ReplicatedKVService::Scan(
    grpc::ServerContext* /*context*/,
    const kv::ScanRequest* request,
    kv::ScanReply* reply
) {
    try {
        const grpc::Status barrier =
            ExecuteReadBarrier();

        if (!barrier.ok()) {
            return barrier;
        }

        std::lock_guard<std::mutex> lock(state_mutex_);

        reply->clear_list();

        const auto list = store_.Scan(
            request->start_key(),
            request->end_key()
        );

        for (const auto& pair : list) {
            auto* output = reply->add_list();
            output->set_key(pair.first);
            output->set_value(pair.second);
        }

        return grpc::Status::OK;
    } catch (const std::exception& error) {
        return ErrorStatus(error);
    }
}

grpc::Status ReplicatedKVService::Delete(
    grpc::ServerContext* /*context*/,
    const kv::DeleteRequest* request,
    kv::DeleteReply* reply
) {
    try {
        MutationReply result;
        const grpc::Status status =
            ExecuteMutation(
                MakeMutationCommand(
                    storage::MUTATION_TYPE_DELETE,
                    request->key(),
                    "",
                    request->header()
                ),
                &result
            );

        if (status.ok()) {
            reply->set_found(result.found);
        }

        return status;
    } catch (const std::exception& error) {
        return ErrorStatus(error);
    }
}

}  // namespace madkv::kvraft
