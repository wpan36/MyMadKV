#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cluster.grpc.pb.h"
#include "cluster.pb.h"

class ClusterClient final {
public:
    // manager_addresses is a comma-separated list. Mandatory P3 uses only
    // manager replica 0, but accepting the whole list keeps the official
    // P3 command-line interface unchanged.
    explicit ClusterClient(
        const std::string& manager_addresses
    );

    ClusterClient(const ClusterClient&) = delete;
    ClusterClient& operator=(const ClusterClient&) = delete;

    madkv::cluster::ClusterConfig
    GetClusterUntilAvailable();

    madkv::cluster::ClusterConfig
    GetClusterUntilReady();

    madkv::cluster::ClusterConfig
    RegisterServerUntilSuccess(
        std::uint32_t partition_id,
        std::uint32_t replica_id,
        const std::string& public_address
    );

private:
    struct ManagerEndpoint {
        std::string address;
        std::unique_ptr<
            madkv::cluster::ClusterManager::Stub
        > stub;
    };

    std::vector<ManagerEndpoint> managers_;
};
