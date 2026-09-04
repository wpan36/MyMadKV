#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "raft.grpc.pb.h"
#include "raft_storage.h"

namespace madkv::raftcore {

enum class RaftRole {
    Follower,
    Candidate,
    Leader,
};

enum class ProposeStatus {
    Committed,
    NotLeader,
    TimedOut,
    Stopped,
};

struct ProposeResult {
    ProposeStatus status = ProposeStatus::Stopped;
    std::uint64_t index = 0;
    std::uint64_t term = 0;
    std::optional<std::uint32_t> leader_id;

    bool committed() const {
        return status == ProposeStatus::Committed;
    }
};

class RaftNode final : public madkv::raft::RaftPeer::Service {
public:
    struct Config {
        std::uint32_t replica_id = 0;
        std::string listen_address;
        std::vector<std::string> peer_addresses;

        std::chrono::milliseconds election_timeout_min{800};
        std::chrono::milliseconds election_timeout_max{1400};
        std::chrono::milliseconds heartbeat_interval{180};
        std::chrono::milliseconds rpc_timeout{300};

        // A single AppendEntries RPC carries at most this many entries.
        // Lagging followers are caught up by scheduling immediate rounds.
        std::size_t max_entries_per_append = 128;
    };

    RaftNode(Config config, RaftStorage& storage);
    ~RaftNode() override;

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    void Start();
    void Stop();

    bool IsLeader() const;
    RaftRole Role() const;
    std::uint64_t CurrentTerm() const;
    std::optional<std::uint32_t> LeaderId() const;

    std::uint64_t LastLogIndex() const;
    std::uint64_t CommitIndex() const;

    // Append a command to the local leader log and wait until the entry is
    // committed by a majority. If leadership is lost first, the caller is
    // told to retry through the current/new leader.
    ProposeResult Propose(
        std::string command,
        std::chrono::milliseconds timeout
    );

    // Utility for the state-machine layer used in Step 4. Committed entries
    // never change, so callers can consume them in increasing index order.
    std::vector<RaftStorage::Entry> GetCommittedEntries(
        std::uint64_t start_index,
        std::size_t max_entries
    ) const;

    bool WaitForCommitIndexAtLeast(
        std::uint64_t index,
        std::chrono::milliseconds timeout
    );

    grpc::Status RequestVote(
        grpc::ServerContext* context,
        const madkv::raft::RequestVoteRequest* request,
        madkv::raft::RequestVoteReply* reply
    ) override;

    grpc::Status AppendEntries(
        grpc::ServerContext* context,
        const madkv::raft::AppendEntriesRequest* request,
        madkv::raft::AppendEntriesReply* reply
    ) override;

private:
    struct Peer {
        std::uint32_t replica_id = 0;
        std::string address;
        std::shared_ptr<grpc::Channel> channel;
        std::unique_ptr<madkv::raft::RaftPeer::Stub> stub;

        // Leader-only volatile state. These values are reset whenever this
        // node becomes leader.
        std::uint64_t next_index = 1;
        std::uint64_t match_index = 0;
    };

    struct VoteRpcResult {
        bool rpc_ok = false;
        madkv::raft::RequestVoteReply reply;
    };

    struct AppendRpcResult {
        bool rpc_ok = false;
        madkv::raft::AppendEntriesReply reply;
    };

    void TimerLoop();
    void StartElection();
    void ReplicateToPeers();

    VoteRpcResult SendRequestVote(
        Peer& peer,
        const madkv::raft::RequestVoteRequest& request
    );

    AppendRpcResult SendAppendEntries(
        Peer& peer,
        const madkv::raft::AppendEntriesRequest& request
    );

    void BecomeFollowerLocked(
        std::uint64_t term,
        std::optional<std::uint32_t> leader_id,
        bool reset_election_timer
    );

    void BecomeLeaderLocked();
    void ResetElectionDeadlineLocked();
    void ScheduleImmediateReplicationLocked();

    bool CandidateLogIsUpToDateLocked(
        std::uint64_t candidate_last_index,
        std::uint64_t candidate_last_term
    ) const;

    bool AdvanceCommitIndexLocked();
    void BacktrackNextIndexLocked(
        Peer& peer,
        const madkv::raft::AppendEntriesReply& reply
    );
    std::optional<std::uint64_t> FindLastIndexOfTermLocked(
        std::uint64_t term
    ) const;

    std::size_t Majority() const;

    Config config_;
    RaftStorage& storage_;
    std::vector<Peer> peers_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool started_ = false;
    bool stopping_ = false;

    RaftRole role_ = RaftRole::Follower;
    std::uint64_t current_term_ = 0;
    std::optional<std::uint32_t> voted_for_;
    std::optional<std::uint32_t> leader_id_;

    std::chrono::steady_clock::time_point election_deadline_;
    std::chrono::steady_clock::time_point heartbeat_deadline_;

    std::mt19937_64 random_;
    std::unique_ptr<grpc::Server> server_;
    std::thread timer_thread_;
};

}  // namespace madkv::raftcore
