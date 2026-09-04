#pragma once

#include <chrono>
#include <cstdint>
#include <exception>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "durable_command.pb.h"
#include "in_memory_kvstore.grpc.pb.h"
#include "in_memory_kvstore.h"
#include "raft_node.h"

namespace madkv::kvraft {

class ReplicatedKVService final
    : public kvstore_service::Operation::Service {
public:
    ReplicatedKVService(
        InMemoryKVStore& store,
        madkv::raftcore::RaftNode& raft,
        std::uint32_t partition_id,
        std::uint32_t partition_count
    );

    // Rebuild the volatile KV state machine and dedup table from the
    // committed prefix of the durable Raft log.
    void RecoverCommittedState();

    std::uint64_t LastApplied() const;

    grpc::Status Put(
        grpc::ServerContext* context,
        const kvstore_service::PutRequest* request,
        kvstore_service::PutReply* reply
    ) override;

    grpc::Status Swap(
        grpc::ServerContext* context,
        const kvstore_service::SwapRequest* request,
        kvstore_service::SwapReply* reply
    ) override;

    grpc::Status Get(
        grpc::ServerContext* context,
        const kvstore_service::GetRequest* request,
        kvstore_service::GetReply* reply
    ) override;

    grpc::Status Scan(
        grpc::ServerContext* context,
        const kvstore_service::ScanRequest* request,
        kvstore_service::ScanReply* reply
    ) override;

    grpc::Status Delete(
        grpc::ServerContext* context,
        const kvstore_service::DeleteRequest* request,
        kvstore_service::DeleteReply* reply
    ) override;

private:
    using Command = madkv::storage::DurableCommand;

    struct MutationReply {
        bool found = false;
        std::string value;
    };

    struct DedupRecord {
        madkv::storage::MutationType type =
            madkv::storage::MUTATION_TYPE_UNSPECIFIED;
        std::string key;
        std::string value;
        MutationReply reply;
        std::uint64_t raft_index = 0;
    };

    class DedupTable final {
    public:
        std::optional<MutationReply> Lookup(
            const Command& command
        ) const;

        void Remember(
            const Command& command,
            const MutationReply& reply,
            std::uint64_t raft_index
        );

    private:
        std::unordered_map<std::string, DedupRecord> records_;
    };

    static constexpr std::chrono::milliseconds kProposeTimeout{5000};

    static bool SameReply(
        const MutationReply& left,
        const MutationReply& right
    );

    static std::string RequestKey(
        const std::string& client_id,
        std::uint64_t request_id
    );

    static void ValidateRequestHeader(
        const kvstore_service::RequestHeader& header
    );

    static Command MakeMutationCommand(
        madkv::storage::MutationType type,
        const std::string& key,
        const std::string& value,
        const kvstore_service::RequestHeader& header
    );

    static std::string SerializeCommand(
        const Command& command
    );

    static grpc::Status StatusFromProposeResult(
        const madkv::raftcore::ProposeResult& result
    );

    static grpc::Status ErrorStatus(
        const std::exception& error
    );

    void EnsureOwnsKey(const std::string& key) const;

    MutationReply ApplyMutationLocked(
        const Command& command
    );

    void ApplyOneCommittedEntryLocked(
        const madkv::raftcore::RaftStorage::Entry& entry
    );

    void ApplyCommittedThrough(std::uint64_t target_index);

    grpc::Status ExecuteMutation(
        Command command,
        MutationReply* reply
    );

    grpc::Status ExecuteReadBarrier();

    InMemoryKVStore& store_;
    madkv::raftcore::RaftNode& raft_;
    DedupTable dedup_table_;

    const std::uint32_t partition_id_;
    const std::uint32_t partition_count_;

    mutable std::mutex state_mutex_;
    std::uint64_t last_applied_ = 0;
};

}  // namespace madkv::kvraft
