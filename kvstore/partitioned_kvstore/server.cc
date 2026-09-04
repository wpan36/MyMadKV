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

#include "cluster.pb.h"
#include "cluster_client.h"
#include "in_memory_kvstore.h"
#include "raft_node.h"
#include "raft_storage.h"
#include "replicated_kv_service.h"

namespace cluster = madkv::cluster;

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

    if (input.empty()) {
        throw std::invalid_argument(
            "peer address list must be 'none' or non-empty"
        );
    }

    std::vector<std::string> peers;
    std::size_t begin = 0;

    while (begin <= input.size()) {
        const std::size_t comma =
            input.find(',', begin);

        const std::size_t end =
            comma == std::string::npos
                ? input.size()
                : comma;

        const std::string peer =
            input.substr(begin, end - begin);

        if (peer.empty()) {
            throw std::invalid_argument(
                "peer address list contains an empty address"
            );
        }

        peers.push_back(peer);

        if (comma == std::string::npos) {
            break;
        }

        begin = comma + 1;
    }

    return peers;
}

struct Assignment {
    std::uint32_t partition_count = 0;
    std::uint32_t server_rf = 0;
    std::string public_address;
};

Assignment ValidateConfig(
    const cluster::ClusterConfig& config,
    const std::uint32_t partition_id,
    const std::uint32_t replica_id,
    const std::size_t peer_count
) {
    if (config.partition_count() == 0) {
        throw std::runtime_error(
            "manager returned partition_count=0"
        );
    }

    if (
        config.server_rf() == 0 ||
        config.server_rf() > 9 ||
        (config.server_rf() % 2) == 0
    ) {
        throw std::runtime_error(
            "manager returned invalid server_rf"
        );
    }

    if (
        config.partitioning_scheme() !=
        cluster::PARTITIONING_SCHEME_FNV1A_64_MOD_N
    ) {
        throw std::runtime_error(
            "manager returned unsupported partitioning scheme"
        );
    }

    if (
        partition_id >= config.partition_count() ||
        replica_id >= config.server_rf()
    ) {
        throw std::runtime_error(
            "this partition_id/replica_id is outside topology"
        );
    }

    if (peer_count + 1 != config.server_rf()) {
        throw std::runtime_error(
            "peer_addrs count does not match manager server_rf"
        );
    }

    const std::size_t expected =
        static_cast<std::size_t>(
            config.partition_count()
        ) *
        config.server_rf();

    if (
        config.replicas_size() !=
        static_cast<int>(expected)
    ) {
        throw std::runtime_error(
            "manager replica list size is inconsistent"
        );
    }

    std::vector<bool> seen(expected, false);
    std::string public_address;

    for (const cluster::ReplicaInfo& info :
         config.replicas()) {
        if (
            info.partition_id() >=
                config.partition_count() ||
            info.replica_id() >=
                config.server_rf()
        ) {
            throw std::runtime_error(
                "manager returned out-of-range replica coordinates"
            );
        }

        const std::size_t flat_index =
            static_cast<std::size_t>(
                info.partition_id()
            ) *
            config.server_rf() +
            info.replica_id();

        if (seen[flat_index]) {
            throw std::runtime_error(
                "manager returned duplicate replica coordinates"
            );
        }

        seen[flat_index] = true;

        if (info.address().empty()) {
            throw std::runtime_error(
                "manager returned an empty server address"
            );
        }

        if (
            info.partition_id() == partition_id &&
            info.replica_id() == replica_id
        ) {
            public_address = info.address();
        }
    }

    for (const bool present : seen) {
        if (!present) {
            throw std::runtime_error(
                "manager configuration is missing a replica"
            );
        }
    }

    if (public_address.empty()) {
        throw std::runtime_error(
            "manager configuration does not contain this replica"
        );
    }

    return Assignment{
        config.partition_count(),
        config.server_rf(),
        public_address
    };
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 8) {
        std::fprintf(
            stderr,
            "Usage: %s <partition_id> <replica_id> "
            "<manager_addrs> <api_listen> <p2p_listen> "
            "<peer_addrs|none> <backer_path>\n",
            argv[0]
        );
        return 1;
    }

    try {
        const std::uint32_t partition_id =
            ParseUint32(argv[1], "partition_id");

        const std::uint32_t replica_id =
            ParseUint32(argv[2], "replica_id");

        const std::string manager_addresses = argv[3];
        const std::string api_listen = argv[4];
        const std::string p2p_listen = argv[5];

        const std::vector<std::string> peers =
            ParsePeers(argv[6]);

        const std::string backer_path = argv[7];

        ClusterClient cluster_client(manager_addresses);

        const cluster::ClusterConfig initial_config =
            cluster_client.GetClusterUntilAvailable();

        const Assignment assignment =
            ValidateConfig(
                initial_config,
                partition_id,
                replica_id,
                peers.size()
            );

        madkv::raftcore::RaftStorage raft_storage(
            backer_path + "/raft"
        );

        madkv::raftcore::RaftNode::Config raft_config;
        raft_config.replica_id = replica_id;
        raft_config.listen_address = p2p_listen;
        raft_config.peer_addresses = peers;

        madkv::raftcore::RaftNode raft(
            std::move(raft_config),
            raft_storage
        );

        InMemoryKVStore store;

        madkv::kvraft::ReplicatedKVService kv_service(
            store,
            raft,
            partition_id,
            assignment.partition_count
        );

        kv_service.RecoverCommittedState();
        raft.Start();

        grpc::ServerBuilder builder;
        builder.AddListeningPort(
            api_listen,
            grpc::InsecureServerCredentials()
        );
        builder.RegisterService(&kv_service);

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

        const cluster::ClusterConfig registered =
            cluster_client.RegisterServerUntilSuccess(
                partition_id,
                replica_id,
                assignment.public_address
            );

        const Assignment registered_assignment =
            ValidateConfig(
                registered,
                partition_id,
                replica_id,
                peers.size()
            );

        if (
            registered_assignment.partition_count !=
                assignment.partition_count ||
            registered_assignment.server_rf !=
                assignment.server_rf ||
            registered_assignment.public_address !=
                assignment.public_address
        ) {
            api_server->Shutdown();
            raft.Stop();

            throw std::runtime_error(
                "cluster topology changed during server startup"
            );
        }

        std::fprintf(
            stderr,
            "KV replica running: partition=%u/%u "
            "replica=%u/%u api=%s public=%s p2p=%s "
            "last_applied=%llu cluster_ready=%s\n",
            partition_id,
            assignment.partition_count,
            replica_id,
            assignment.server_rf,
            api_listen.c_str(),
            assignment.public_address.c_str(),
            p2p_listen.c_str(),
            static_cast<unsigned long long>(
                kv_service.LastApplied()
            ),
            registered.ready() ? "true" : "false"
        );

        api_server->Wait();
        raft.Stop();

        return 0;
    } catch (const std::exception& error) {
        std::fprintf(
            stderr,
            "Server failed: %s\n",
            error.what()
        );
        return 2;
    }
}
