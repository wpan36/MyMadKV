#include "cluster_client.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>

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

std::runtime_error PermanentRpcError(
    const std::string& operation,
    const grpc::Status& status
) {
    return std::runtime_error(
        operation +
        " failed with gRPC code " +
        std::to_string(status.error_code()) +
        ": " +
        status.error_message()
    );
}

void PrintRetry(
    const std::string& operation,
    const std::string& manager_address,
    const grpc::Status& status,
    const std::chrono::milliseconds delay
) {
    std::fprintf(
        stderr,
        "%s to manager %s temporarily failed: %s; "
        "retrying in %lld ms\n",
        operation.c_str(),
        manager_address.c_str(),
        status.error_message().c_str(),
        static_cast<long long>(delay.count())
    );
}

}  // namespace

ClusterClient::ClusterClient(
    const std::string& manager_address
)
    : manager_address_(manager_address) {
    if (manager_address_.empty()) {
        throw std::invalid_argument(
            "manager address must not be empty"
        );
    }

    auto channel = grpc::CreateChannel(
        manager_address_,
        grpc::InsecureChannelCredentials()
    );

    stub_ = cluster::ClusterManager::NewStub(channel);
}

cluster::ClusterConfig
ClusterClient::GetClusterUntilAvailable() {
    using namespace std::chrono_literals;

    auto retry_delay = 100ms;
    constexpr auto max_retry_delay = 1000ms;

    while (true) {
        cluster::GetClusterRequest request;
        cluster::GetClusterReply reply;

        grpc::ClientContext context;

        context.set_deadline(
            std::chrono::system_clock::now() + 1s
        );

        const grpc::Status status =
            stub_->GetCluster(
                &context,
                request,
                &reply
            );

        if (status.ok()) {
            return reply.config();
        }

        if (!IsRetryable(status.error_code())) {
            throw PermanentRpcError(
                "GetCluster",
                status
            );
        }

        PrintRetry(
            "GetCluster",
            manager_address_,
            status,
            retry_delay
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
            "Cluster is not ready yet; waiting for "
            "all partition servers to register\n"
        );

        std::this_thread::sleep_for(200ms);
    }
}

cluster::ClusterConfig
ClusterClient::RegisterServerUntilSuccess(
    const std::uint32_t server_id,
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
        cluster::RegisterServerRequest request;
        cluster::RegisterServerReply reply;

        request.set_server_id(server_id);
        request.set_address(public_address);

        grpc::ClientContext context;

        context.set_deadline(
            std::chrono::system_clock::now() + 1s
        );

        const grpc::Status status =
            stub_->RegisterServer(
                &context,
                request,
                &reply
            );

        if (status.ok()) {
            return reply.config();
        }

        if (!IsRetryable(status.error_code())) {
            throw PermanentRpcError(
                "RegisterServer",
                status
            );
        }

        PrintRetry(
            "RegisterServer",
            manager_address_,
            status,
            retry_delay
        );

        std::this_thread::sleep_for(retry_delay);

        retry_delay = std::min(
            retry_delay * 2,
            max_retry_delay
        );
    }
}
