#include "raft_node.h"

#include <algorithm>
#include <cstdio>
#include <future>
#include <stdexcept>
#include <utility>

namespace madkv::raftcore {
namespace {

using Clock = std::chrono::steady_clock;

}  // namespace

RaftNode::RaftNode(
    Config config,
    RaftStorage& storage
)
    : config_(std::move(config)),
      storage_(storage),
      current_term_(storage_.CurrentTerm()),
      voted_for_(storage_.VotedFor()),
      random_(
          static_cast<std::uint64_t>(
              std::chrono::high_resolution_clock::now()
                  .time_since_epoch()
                  .count()
          ) ^
          (static_cast<std::uint64_t>(config_.replica_id) << 32U)
      ) {
    if (config_.listen_address.empty()) {
        throw std::invalid_argument(
            "Raft p2p listen address must not be empty"
        );
    }

    if (
        config_.election_timeout_min.count() <= 0 ||
        config_.election_timeout_max <= config_.election_timeout_min ||
        config_.heartbeat_interval.count() <= 0 ||
        config_.rpc_timeout.count() <= 0
    ) {
        throw std::invalid_argument(
            "Invalid Raft timeout configuration"
        );
    }

    const std::size_t replica_count =
        config_.peer_addresses.size() + 1;

    if (
        replica_count == 0 ||
        replica_count > 9 ||
        (replica_count % 2) == 0
    ) {
        throw std::invalid_argument(
            "Raft replication factor must be an odd number from 1 to 9"
        );
    }

    if (config_.replica_id >= replica_count) {
        throw std::invalid_argument(
            "Raft replica_id is outside the replica group"
        );
    }

    peers_.reserve(config_.peer_addresses.size());

    for (
        std::size_t peer_position = 0;
        peer_position < config_.peer_addresses.size();
        ++peer_position
    ) {
        const std::string& address =
            config_.peer_addresses[peer_position];

        if (address.empty()) {
            throw std::invalid_argument(
                "Raft peer address must not be empty"
            );
        }

        const std::uint32_t peer_id =
            static_cast<std::uint32_t>(
                peer_position < config_.replica_id
                    ? peer_position
                    : peer_position + 1
            );

        auto channel = grpc::CreateChannel(
            address,
            grpc::InsecureChannelCredentials()
        );

        Peer peer;
        peer.replica_id = peer_id;
        peer.address = address;
        peer.channel = channel;
        peer.stub = madkv::raft::RaftPeer::NewStub(channel);

        peers_.push_back(std::move(peer));
    }
}

RaftNode::~RaftNode() {
    Stop();
}

void RaftNode::Start() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (started_) {
        return;
    }

    grpc::ServerBuilder builder;
    builder.AddListeningPort(
        config_.listen_address,
        grpc::InsecureServerCredentials()
    );
    builder.RegisterService(this);

    server_ = builder.BuildAndStart();

    if (!server_) {
        throw std::runtime_error(
            "Failed to start Raft p2p gRPC server on " +
            config_.listen_address
        );
    }

    stopping_ = false;
    role_ = RaftRole::Follower;
    leader_id_.reset();
    current_term_ = storage_.CurrentTerm();
    voted_for_ = storage_.VotedFor();

    ResetElectionDeadlineLocked();
    heartbeat_deadline_ = Clock::now();

    started_ = true;

    const std::string vote_text =
        voted_for_.has_value()
            ? std::to_string(*voted_for_)
            : "none";

    std::fprintf(
        stderr,
        "Raft replica %u started on %s: term=%llu "
        "voted_for=%s last_log=%llu/%llu commit=%llu\n",
        config_.replica_id,
        config_.listen_address.c_str(),
        static_cast<unsigned long long>(current_term_),
        vote_text.c_str(),
        static_cast<unsigned long long>(
            storage_.LastLogIndex()
        ),
        static_cast<unsigned long long>(
            storage_.LastLogTerm()
        ),
        static_cast<unsigned long long>(
            storage_.CommitIndex()
        )
    );

    timer_thread_ = std::thread(
        &RaftNode::TimerLoop,
        this
    );
}

void RaftNode::Stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!started_) {
            return;
        }

        stopping_ = true;
        cv_.notify_all();
    }

    if (server_) {
        server_->Shutdown();
    }

    if (timer_thread_.joinable()) {
        timer_thread_.join();
    }

    std::lock_guard<std::mutex> lock(mutex_);
    server_.reset();
    started_ = false;
}

bool RaftNode::IsLeader() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return role_ == RaftRole::Leader;
}

RaftRole RaftNode::Role() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return role_;
}

std::uint64_t RaftNode::CurrentTerm() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_term_;
}

std::optional<std::uint32_t> RaftNode::LeaderId() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return leader_id_;
}

grpc::Status RaftNode::RequestVote(
    grpc::ServerContext* /*context*/,
    const madkv::raft::RequestVoteRequest* request,
    madkv::raft::RequestVoteReply* reply
) {
    try {
        std::lock_guard<std::mutex> lock(mutex_);

        if (request->term() < current_term_) {
            reply->set_term(current_term_);
            reply->set_vote_granted(false);
            return grpc::Status::OK;
        }

        if (
            request->candidate_id() >=
            peers_.size() + 1
        ) {
            return grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "candidate_id is outside the replica group"
            );
        }

        if (request->term() > current_term_) {
            BecomeFollowerLocked(
                request->term(),
                std::nullopt,
                false
            );
        }

        const bool can_vote =
            !voted_for_.has_value() ||
            *voted_for_ == request->candidate_id();

        const bool log_is_up_to_date =
            CandidateLogIsUpToDateLocked(
                request->last_log_index(),
                request->last_log_term()
            );

        const bool grant = can_vote && log_is_up_to_date;

        if (grant) {
            if (
                !voted_for_.has_value() ||
                *voted_for_ != request->candidate_id()
            ) {
                storage_.SetVotedFor(
                    request->candidate_id()
                );
                voted_for_ = request->candidate_id();
            }

            leader_id_.reset();
            ResetElectionDeadlineLocked();
        }

        reply->set_term(current_term_);
        reply->set_vote_granted(grant);
        return grpc::Status::OK;
    } catch (const std::exception& error) {
        return grpc::Status(
            grpc::StatusCode::INTERNAL,
            error.what()
        );
    }
}

grpc::Status RaftNode::AppendEntries(
    grpc::ServerContext* /*context*/,
    const madkv::raft::AppendEntriesRequest* request,
    madkv::raft::AppendEntriesReply* reply
) {
    try {
        std::lock_guard<std::mutex> lock(mutex_);

        reply->set_term(current_term_);
        reply->set_success(false);
        reply->set_match_index(0);
        reply->set_conflict_index(0);
        reply->set_conflict_term(0);

        if (request->term() < current_term_) {
            return grpc::Status::OK;
        }

        if (
            request->leader_id() >=
            peers_.size() + 1
        ) {
            return grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "leader_id is outside the replica group"
            );
        }

        if (
            request->term() > current_term_ ||
            role_ != RaftRole::Follower
        ) {
            BecomeFollowerLocked(
                request->term(),
                request->leader_id(),
                true
            );
        } else {
            leader_id_ = request->leader_id();
            ResetElectionDeadlineLocked();
        }

        reply->set_term(current_term_);

        const std::uint64_t local_last_index =
            storage_.LastLogIndex();

        if (request->prev_log_index() > local_last_index) {
            reply->set_conflict_index(
                local_last_index + 1
            );
            return grpc::Status::OK;
        }

        if (request->prev_log_index() > 0) {
            const auto previous =
                storage_.GetEntry(
                    request->prev_log_index()
                );

            if (!previous.has_value()) {
                reply->set_conflict_index(
                    local_last_index + 1
                );
                return grpc::Status::OK;
            }

            if (
                previous->term() !=
                request->prev_log_term()
            ) {
                const std::uint64_t conflict_term =
                    previous->term();

                std::uint64_t first_index =
                    request->prev_log_index();

                while (first_index > 1) {
                    const auto prior =
                        storage_.GetEntry(
                            first_index - 1
                        );

                    if (
                        !prior.has_value() ||
                        prior->term() != conflict_term
                    ) {
                        break;
                    }

                    --first_index;
                }

                reply->set_conflict_term(conflict_term);
                reply->set_conflict_index(first_index);
                return grpc::Status::OK;
            }
        }

        std::uint64_t expected_index =
            request->prev_log_index() + 1;

        for (int i = 0; i < request->entries_size(); ++i) {
            const auto& incoming = request->entries(i);

            if (incoming.index() != expected_index) {
                return grpc::Status(
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "AppendEntries contains non-contiguous indices"
                );
            }

            ++expected_index;
        }

        int first_new_entry = request->entries_size();

        for (int i = 0; i < request->entries_size(); ++i) {
            const auto& incoming = request->entries(i);
            const auto existing =
                storage_.GetEntry(incoming.index());

            if (!existing.has_value()) {
                first_new_entry = i;
                break;
            }

            if (existing->term() != incoming.term()) {
                storage_.TruncateSuffix(incoming.index());
                first_new_entry = i;
                break;
            }

            if (existing->command() != incoming.command()) {
                return grpc::Status(
                    grpc::StatusCode::INTERNAL,
                    "Raft log matching violation: same index/term "
                    "contains different commands"
                );
            }
        }

        if (first_new_entry < request->entries_size()) {
            std::vector<RaftStorage::Entry> suffix;
            suffix.reserve(
                static_cast<std::size_t>(
                    request->entries_size() -
                    first_new_entry
                )
            );

            for (
                int i = first_new_entry;
                i < request->entries_size();
                ++i
            ) {
                suffix.push_back(request->entries(i));
            }

            storage_.AppendEntries(suffix);
        }

        if (
            request->leader_commit() >
            storage_.CommitIndex()
        ) {
            const std::uint64_t new_commit =
                std::min(
                    request->leader_commit(),
                    storage_.LastLogIndex()
                );

            storage_.SetCommitIndex(new_commit);
        }

        const std::uint64_t match_index =
            request->prev_log_index() +
            static_cast<std::uint64_t>(
                request->entries_size()
            );

        reply->set_success(true);
        reply->set_match_index(match_index);
        reply->set_conflict_index(0);
        reply->set_conflict_term(0);

        return grpc::Status::OK;
    } catch (const std::exception& error) {
        return grpc::Status(
            grpc::StatusCode::INTERNAL,
            error.what()
        );
    }
}

void RaftNode::TimerLoop() {
    std::unique_lock<std::mutex> lock(mutex_);

    while (!stopping_) {
        if (role_ == RaftRole::Leader) {
            const auto deadline = heartbeat_deadline_;

            cv_.wait_until(
                lock,
                deadline,
                [this, deadline] {
                    return
                        stopping_ ||
                        role_ != RaftRole::Leader ||
                        heartbeat_deadline_ != deadline;
                }
            );

            if (stopping_) {
                break;
            }

            if (role_ != RaftRole::Leader) {
                continue;
            }

            if (Clock::now() < heartbeat_deadline_) {
                continue;
            }

            heartbeat_deadline_ =
                Clock::now() +
                config_.heartbeat_interval;

            lock.unlock();
            SendHeartbeats();
            lock.lock();
            continue;
        }

        const auto deadline = election_deadline_;

        cv_.wait_until(
            lock,
            deadline,
            [this, deadline] {
                return
                    stopping_ ||
                    role_ == RaftRole::Leader ||
                    election_deadline_ != deadline;
            }
        );

        if (stopping_) {
            break;
        }

        if (role_ == RaftRole::Leader) {
            continue;
        }

        if (Clock::now() < election_deadline_) {
            continue;
        }

        lock.unlock();
        StartElection();
        lock.lock();
    }
}

void RaftNode::StartElection() {
    madkv::raft::RequestVoteRequest request;
    std::uint64_t election_term = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (stopping_ || role_ == RaftRole::Leader) {
            return;
        }

        role_ = RaftRole::Candidate;
        leader_id_.reset();

        election_term = current_term_ + 1;
        storage_.SetCurrentTermAndVote(
            election_term,
            config_.replica_id
        );

        current_term_ = election_term;
        voted_for_ = config_.replica_id;

        request.set_term(election_term);
        request.set_candidate_id(config_.replica_id);
        request.set_last_log_index(
            storage_.LastLogIndex()
        );
        request.set_last_log_term(
            storage_.LastLogTerm()
        );

        ResetElectionDeadlineLocked();

        std::fprintf(
            stderr,
            "Raft replica %u starts election for term %llu\n",
            config_.replica_id,
            static_cast<unsigned long long>(
                election_term
            )
        );
    }

    std::size_t votes = 1;

    if (votes >= Majority()) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (
            role_ == RaftRole::Candidate &&
            current_term_ == election_term
        ) {
            BecomeLeaderLocked();
        }

        return;
    }

    std::vector<std::future<VoteRpcResult>> futures;
    futures.reserve(peers_.size());

    for (Peer& peer : peers_) {
        Peer* const peer_ptr = &peer;

        futures.push_back(
            std::async(
                std::launch::async,
                [this, peer_ptr, request] {
                    return SendRequestVote(
                        *peer_ptr,
                        request
                    );
                }
            )
        );
    }

    for (auto& future : futures) {
        const VoteRpcResult result = future.get();

        if (!result.rpc_ok) {
            continue;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        if (result.reply.term() > current_term_) {
            BecomeFollowerLocked(
                result.reply.term(),
                std::nullopt,
                true
            );
            return;
        }

        if (
            role_ != RaftRole::Candidate ||
            current_term_ != election_term
        ) {
            return;
        }

        if (
            result.reply.term() == election_term &&
            result.reply.vote_granted()
        ) {
            ++votes;

            if (votes >= Majority()) {
                BecomeLeaderLocked();
                return;
            }
        }
    }
}

void RaftNode::SendHeartbeats() {
    madkv::raft::AppendEntriesRequest request;
    std::uint64_t heartbeat_term = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (
            stopping_ ||
            role_ != RaftRole::Leader
        ) {
            return;
        }

        heartbeat_term = current_term_;

        const std::uint64_t last_index =
            storage_.LastLogIndex();

        request.set_term(heartbeat_term);
        request.set_leader_id(config_.replica_id);
        request.set_prev_log_index(last_index);
        request.set_prev_log_term(
            storage_.LastLogTerm()
        );
        request.set_leader_commit(
            storage_.CommitIndex()
        );
    }

    std::vector<std::future<AppendRpcResult>> futures;
    futures.reserve(peers_.size());

    for (Peer& peer : peers_) {
        Peer* const peer_ptr = &peer;

        futures.push_back(
            std::async(
                std::launch::async,
                [this, peer_ptr, request] {
                    return SendAppendEntries(
                        *peer_ptr,
                        request
                    );
                }
            )
        );
    }

    for (auto& future : futures) {
        const AppendRpcResult result = future.get();

        if (!result.rpc_ok) {
            continue;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        if (result.reply.term() > current_term_) {
            BecomeFollowerLocked(
                result.reply.term(),
                std::nullopt,
                true
            );
            return;
        }

        if (
            role_ != RaftRole::Leader ||
            current_term_ != heartbeat_term
        ) {
            return;
        }

        // A false reply means the follower log is behind/conflicting.
        // Step 3 will use nextIndex/matchIndex to repair it.
    }
}

RaftNode::VoteRpcResult RaftNode::SendRequestVote(
    Peer& peer,
    const madkv::raft::RequestVoteRequest& request
) {
    grpc::ClientContext context;
    context.set_deadline(
        std::chrono::system_clock::now() +
        config_.rpc_timeout
    );

    VoteRpcResult result;
    const grpc::Status status =
        peer.stub->RequestVote(
            &context,
            request,
            &result.reply
        );

    result.rpc_ok = status.ok();
    return result;
}

RaftNode::AppendRpcResult RaftNode::SendAppendEntries(
    Peer& peer,
    const madkv::raft::AppendEntriesRequest& request
) {
    grpc::ClientContext context;
    context.set_deadline(
        std::chrono::system_clock::now() +
        config_.rpc_timeout
    );

    AppendRpcResult result;
    const grpc::Status status =
        peer.stub->AppendEntries(
            &context,
            request,
            &result.reply
        );

    result.rpc_ok = status.ok();
    return result;
}

void RaftNode::BecomeFollowerLocked(
    const std::uint64_t term,
    const std::optional<std::uint32_t> leader_id,
    const bool reset_election_timer
) {
    if (term < current_term_) {
        return;
    }

    if (term > current_term_) {
        storage_.SetCurrentTermAndVote(
            term,
            std::nullopt
        );

        current_term_ = term;
        voted_for_.reset();
    }

    if (role_ != RaftRole::Follower) {
        std::fprintf(
            stderr,
            "Raft replica %u becomes follower in term %llu\n",
            config_.replica_id,
            static_cast<unsigned long long>(
                current_term_
            )
        );
    }

    role_ = RaftRole::Follower;
    leader_id_ = leader_id;

    if (reset_election_timer) {
        ResetElectionDeadlineLocked();
    } else {
        cv_.notify_all();
    }
}

void RaftNode::BecomeLeaderLocked() {
    role_ = RaftRole::Leader;
    leader_id_ = config_.replica_id;
    heartbeat_deadline_ = Clock::now();

    std::fprintf(
        stderr,
        "Raft replica %u becomes LEADER for term %llu\n",
        config_.replica_id,
        static_cast<unsigned long long>(
            current_term_
        )
    );

    cv_.notify_all();
}

void RaftNode::ResetElectionDeadlineLocked() {
    const auto minimum =
        config_.election_timeout_min.count();
    const auto maximum =
        config_.election_timeout_max.count();

    std::uniform_int_distribution<long long> distribution(
        minimum,
        maximum
    );

    election_deadline_ =
        Clock::now() +
        std::chrono::milliseconds(
            distribution(random_)
        );

    cv_.notify_all();
}

bool RaftNode::CandidateLogIsUpToDateLocked(
    const std::uint64_t candidate_last_index,
    const std::uint64_t candidate_last_term
) const {
    const std::uint64_t local_last_term =
        storage_.LastLogTerm();

    if (candidate_last_term != local_last_term) {
        return candidate_last_term > local_last_term;
    }

    return
        candidate_last_index >=
        storage_.LastLogIndex();
}

std::size_t RaftNode::Majority() const {
    const std::size_t replica_count =
        peers_.size() + 1;

    return replica_count / 2 + 1;
}

}  // namespace madkv::raftcore
