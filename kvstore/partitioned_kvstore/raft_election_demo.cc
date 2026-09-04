#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "raft_node.h"
#include "raft_storage.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int /*signal*/) {
    g_stop.store(true);
}

std::uint32_t ParseReplicaId(const std::string& input) {
    std::size_t consumed = 0;
    unsigned long value = 0;

    try {
        value = std::stoul(input, &consumed, 10);
    } catch (const std::exception&) {
        throw std::invalid_argument(
            "replica_id must be a non-negative integer"
        );
    }

    if (
        consumed != input.size() ||
        value > std::numeric_limits<std::uint32_t>::max()
    ) {
        throw std::invalid_argument(
            "invalid replica_id"
        );
    }

    return static_cast<std::uint32_t>(value);
}

std::vector<std::string> ParsePeers(const std::string& input) {
    if (input == "none") {
        return {};
    }

    std::vector<std::string> peers;
    std::size_t start = 0;

    while (start <= input.size()) {
        const std::size_t comma = input.find(',', start);
        const std::size_t end =
            comma == std::string::npos
                ? input.size()
                : comma;

        const std::string peer =
            input.substr(start, end - start);

        if (peer.empty()) {
            throw std::invalid_argument(
                "peer address list contains an empty item"
            );
        }

        peers.push_back(peer);

        if (comma == std::string::npos) {
            break;
        }

        start = comma + 1;
    }

    return peers;
}

const char* ProposeStatusName(
    const madkv::raftcore::ProposeStatus status
) {
    using madkv::raftcore::ProposeStatus;

    switch (status) {
        case ProposeStatus::Committed:
            return "COMMITTED";
        case ProposeStatus::NotLeader:
            return "NOT_LEADER";
        case ProposeStatus::TimedOut:
            return "TIMED_OUT";
        case ProposeStatus::Stopped:
            return "STOPPED";
    }

    return "UNKNOWN";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5 && argc != 6) {
        std::fprintf(
            stderr,
            "Usage: %s <replica_id> <p2p_listen> "
            "<peer_addrs|none> <backer_path> [auto_command]\n",
            argv[0]
        );
        return 1;
    }

    try {
        const std::uint32_t replica_id =
            ParseReplicaId(argv[1]);

        madkv::raftcore::RaftStorage storage(
            std::string(argv[4]) + "/raft"
        );

        madkv::raftcore::RaftNode::Config config;
        config.replica_id = replica_id;
        config.listen_address = argv[2];
        config.peer_addresses = ParsePeers(argv[3]);

        madkv::raftcore::RaftNode node(
            std::move(config),
            storage
        );

        std::signal(SIGINT, HandleSignal);
        std::signal(SIGTERM, HandleSignal);

        node.Start();

        const bool auto_propose = argc == 6;
        const std::string auto_command =
            auto_propose ? argv[5] : std::string();

        bool proposal_attempted = false;

        while (!g_stop.load()) {
            if (
                auto_propose &&
                !proposal_attempted &&
                node.IsLeader()
            ) {
                proposal_attempted = true;

                const auto result =
                    node.Propose(
                        auto_command,
                        std::chrono::seconds(8)
                    );

                const std::string leader_text =
                    result.leader_id.has_value()
                        ? std::to_string(*result.leader_id)
                        : "unknown";

                std::fprintf(
                    stderr,
                    "AUTO_PROPOSE replica=%u status=%s "
                    "index=%llu term=%llu leader=%s "
                    "last_log=%llu commit=%llu command='%s'\n",
                    replica_id,
                    ProposeStatusName(result.status),
                    static_cast<unsigned long long>(
                        result.index
                    ),
                    static_cast<unsigned long long>(
                        result.term
                    ),
                    leader_text.c_str(),
                    static_cast<unsigned long long>(
                        node.LastLogIndex()
                    ),
                    static_cast<unsigned long long>(
                        node.CommitIndex()
                    ),
                    auto_command.c_str()
                );
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(50)
            );
        }

        node.Stop();
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(
            stderr,
            "raft_election_demo failed: %s\n",
            error.what()
        );
        return 2;
    }
}
