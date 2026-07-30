#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "cluster.grpc.pb.h"
#include "cluster.pb.h"

class ClusterClient final {
public:
    explicit ClusterClient(
        const std::string& manager_address
    );

    ClusterClient(const ClusterClient&) = delete;
    ClusterClient& operator=(const ClusterClient&) = delete;

    // Returns the static cluster configuration as soon as the
    // manager becomes reachable. The cluster may not yet be ready.
    madkv::cluster::ClusterConfig
    GetClusterUntilAvailable();

    // Waits until every configured partition server is registered.
    madkv::cluster::ClusterConfig
    GetClusterUntilReady();

    // Retries registration while the manager is temporarily unavailable.
    madkv::cluster::ClusterConfig
    RegisterServerUntilSuccess(
        std::uint32_t server_id,
        const std::string& public_address
    );

private:
    std::string manager_address_;

    std::unique_ptr<
        madkv::cluster::ClusterManager::Stub
    > stub_;
};
