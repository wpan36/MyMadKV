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

}  // namespace

int main(int argc, char** argv) {
    if (argc != 5) {
        std::fprintf(
            stderr,
            "Usage: %s <replica_id> <p2p_listen> "
            "<peer_addrs|none> <backer_path>\n",
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

        while (!g_stop.load()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100)
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
