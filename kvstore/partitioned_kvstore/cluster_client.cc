#include "cluster_client.h"

#include <algorithm>
#include <chrono>
#include <optional>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

namespace cluster = madkv::cluster;

namespace {

bool IsRetryable(
    const grpc::StatusCode status_code
) {
    switch (status_code) {
        case grpc::StatusCode::UNAVAILABLE:
        case grpc::StatusCode::DEADLINE_EXCEEDED:
        case grpc::StatusCode::CANCELLED:
        case grpc::StatusCode::UNKNOWN:
        case grpc::StatusCode::ABORTED:
        case grpc::StatusCode::RESOURCE_EXHAUSTED:
            return true;

        default:
            return false;
    }
}

std::vector<std::string> ParseAddresses(
    const std::string& input
) {
    if (input.empty()) {
        throw std::invalid_argument(
            "manager address list must not be empty"
        );
    }

    std::vector<std::string> addresses;
    std::unordered_set<std::string> seen;

    std::size_t begin = 0;

    while (begin <= input.size()) {
        const std::size_t comma =
            input.find(',', begin);

        const std::size_t end =
            comma == std::string::npos
                ? input.size()
                : comma;

        const std::string address =
            input.substr(begin, end - begin);

        if (address.empty()) {
            throw std::invalid_argument(
                "manager address list contains an empty address"
            );
        }

        if (!seen.insert(address).second) {
            throw std::invalid_argument(
                "duplicate manager address: " + address
            );
        }

        addresses.push_back(address);

        if (comma == std::string::npos) {
            break;
        }

        begin = comma + 1;
    }

    return addresses;
}

std::runtime_error PermanentRpcError(
    const std::string& operation,
    const std::string& address,
    const grpc::Status& status
) {
    return std::runtime_error(
        operation +
        " to manager " +
        address +
        " failed with gRPC code " +
        std::to_string(status.error_code()) +
        ": " +
        status.error_message()
    );
}

}  // namespace

ClusterClient::ClusterClient(
    const std::string& manager_addresses
) {
    const auto addresses =
        ParseAddresses(manager_addresses);

    managers_.reserve(addresses.size());

    for (const std::string& address : addresses) {
        auto channel = grpc::CreateChannel(
            address,
            grpc::InsecureChannelCredentials()
        );

        ManagerEndpoint endpoint;
        endpoint.address = address;
        endpoint.stub =
            cluster::ClusterManager::NewStub(channel);

        managers_.push_back(std::move(endpoint));
    }
}

cluster::ClusterConfig
ClusterClient::GetClusterUntilAvailable() {
    using namespace std::chrono_literals;

    auto retry_delay = 100ms;
    constexpr auto max_retry_delay = 1000ms;

    while (true) {
        std::optional<std::runtime_error> permanent_error;

        for (ManagerEndpoint& manager : managers_) {
            cluster::GetClusterRequest request;
            cluster::GetClusterReply reply;
            grpc::ClientContext context;

            context.set_deadline(
                std::chrono::system_clock::now() + 1s
            );

            const grpc::Status status =
                manager.stub->GetCluster(
                    &context,
                    request,
                    &reply
                );

            if (status.ok()) {
                return reply.config();
            }

            if (!IsRetryable(status.error_code())) {
                permanent_error =
                    PermanentRpcError(
                        "GetCluster",
                        manager.address,
                        status
                    );
            }
        }

        if (permanent_error.has_value()) {
            throw *permanent_error;
        }

        std::fprintf(
            stderr,
            "No manager is reachable yet; retrying in %lld ms\n",
            static_cast<long long>(retry_delay.count())
        );

        std::this_thread::sleep_for(retry_delay);

        retry_delay = std::min(
            retry_delay * 2,
            max_retry_delay
        );
    }
}

cluster::ClusterConfig
ClusterClient::GetClusterUntilReady() {
    using namespace std::chrono_literals;

    while (true) {
        cluster::ClusterConfig config =
            GetClusterUntilAvailable();

        if (config.ready()) {
            return config;
        }

        std::fprintf(
            stderr,
            "Cluster is not ready yet; waiting for all "
            "server replicas to register\n"
        );

        std::this_thread::sleep_for(200ms);
    }
}

cluster::ClusterConfig
ClusterClient::RegisterServerUntilSuccess(
    const std::uint32_t partition_id,
    const std::uint32_t replica_id,
    const std::string& public_address
) {
    using namespace std::chrono_literals;

    if (public_address.empty()) {
        throw std::invalid_argument(
            "server public address must not be empty"
        );
    }

    auto retry_delay = 100ms;
    constexpr auto max_retry_delay = 1000ms;

    while (true) {
        std::optional<std::runtime_error> permanent_error;

        for (ManagerEndpoint& manager : managers_) {
            cluster::RegisterServerRequest request;
            cluster::RegisterServerReply reply;

            request.set_partition_id(partition_id);
            request.set_replica_id(replica_id);
            request.set_address(public_address);

            grpc::ClientContext context;
            context.set_deadline(
                std::chrono::system_clock::now() + 1s
            );

            const grpc::Status status =
                manager.stub->RegisterServer(
                    &context,
                    request,
                    &reply
                );

            if (status.ok()) {
                return reply.config();
            }

            if (!IsRetryable(status.error_code())) {
                permanent_error =
                    PermanentRpcError(
                        "RegisterServer",
                        manager.address,
                        status
                    );
            }
        }

        if (permanent_error.has_value()) {
            throw *permanent_error;
        }

        std::fprintf(
            stderr,
            "No manager accepted server registration; "
            "retrying in %lld ms\n",
            static_cast<long long>(retry_delay.count())
        );

        std::this_thread::sleep_for(retry_delay);

        retry_delay = std::min(
            retry_delay * 2,
            max_retry_delay
        );
    }
}
