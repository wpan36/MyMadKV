#include <cstdint>
#include <cstdio>
#include <exception>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "in_memory_kvstore.h"
#include "raft_node.h"
#include "raft_storage.h"
#include "replicated_kv_service.h"

namespace {

std::uint32_t ParseUint32(
    const std::string& input,
    const char* name
) {
    std::size_t consumed = 0;
    unsigned long parsed = 0;

    try {
        parsed = std::stoul(input, &consumed, 10);
    } catch (const std::exception&) {
        throw std::invalid_argument(
            std::string(name) +
            " must be a non-negative integer"
        );
    }

    if (
        consumed != input.size() ||
        parsed >
            std::numeric_limits<std::uint32_t>::max()
    ) {
        throw std::invalid_argument(
            std::string("invalid ") + name
        );
    }

    return static_cast<std::uint32_t>(parsed);
}

std::vector<std::string> ParsePeers(
    const std::string& input
) {
    if (input == "none") {
        return {};
    }

    std::vector<std::string> peers;
    std::size_t start = 0;

    while (start <= input.size()) {
        const std::size_t comma =
            input.find(',', start);

        const std::size_t end =
            comma == std::string::npos
                ? input.size()
                : comma;

        const std::string peer =
            input.substr(start, end - start);

        if (peer.empty()) {
            throw std::invalid_argument(
                "peer list contains an empty address"
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
    if (argc != 8) {
        std::fprintf(
            stderr,
            "Usage: %s <partition_id> <partition_count> "
            "<replica_id> <api_listen> <p2p_listen> "
            "<peer_addrs|none> <backer_path>\n",
            argv[0]
        );
        return 1;
    }

    try {
        const std::uint32_t partition_id =
            ParseUint32(argv[1], "partition_id");

        const std::uint32_t partition_count =
            ParseUint32(argv[2], "partition_count");

        const std::uint32_t replica_id =
            ParseUint32(argv[3], "replica_id");

        const std::string api_listen = argv[4];
        const std::string p2p_listen = argv[5];

        const std::vector<std::string> peers =
            ParsePeers(argv[6]);

        const std::string backer_path = argv[7];

        madkv::raftcore::RaftStorage raft_storage(
            backer_path + "/raft"
        );

        madkv::raftcore::RaftNode::Config config;
        config.replica_id = replica_id;
        config.listen_address = p2p_listen;
        config.peer_addresses = peers;

        madkv::raftcore::RaftNode raft(
            std::move(config),
            raft_storage
        );

        InMemoryKVStore store;

        madkv::kvraft::ReplicatedKVService service(
            store,
            raft,
            partition_id,
            partition_count
        );

        service.RecoverCommittedState();
        raft.Start();

        grpc::ServerBuilder builder;
        builder.AddListeningPort(
            api_listen,
            grpc::InsecureServerCredentials()
        );
        builder.RegisterService(&service);

        std::unique_ptr<grpc::Server> api_server(
            builder.BuildAndStart()
        );

        if (!api_server) {
            raft.Stop();
            throw std::runtime_error(
                "failed to start KV API server on " +
                api_listen
            );
        }

        std::fprintf(
            stderr,
            "Raft KV demo server running: "
            "partition=%u/%u replica=%u api=%s p2p=%s "
            "last_applied=%llu\n",
            partition_id,
            partition_count,
            replica_id,
            api_listen.c_str(),
            p2p_listen.c_str(),
            static_cast<unsigned long long>(
                service.LastApplied()
            )
        );

        api_server->Wait();
        raft.Stop();

        return 0;
    } catch (const std::exception& error) {
        std::fprintf(
            stderr,
            "raft_kv_demo_server failed: %s\n",
            error.what()
        );
        return 2;
    }
}
