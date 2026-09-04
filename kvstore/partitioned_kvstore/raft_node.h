#pragma once

#include <chrono>
#include <condition_variable>
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
    void SendHeartbeats();

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

    bool CandidateLogIsUpToDateLocked(
        std::uint64_t candidate_last_index,
        std::uint64_t candidate_last_term
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
