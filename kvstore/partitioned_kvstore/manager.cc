#include <cstdio>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "cluster.grpc.pb.h"
#include "cluster.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

namespace cluster = madkv::cluster;

namespace {

std::vector<std::string> ParseServerAddresses(
    const std::string& input
) {
    if (input.empty()) {
        throw std::invalid_argument(
            "server address list must not be empty"
        );
    }

    std::vector<std::string> addresses;
    std::unordered_set<std::string> unique_addresses;

    std::size_t begin = 0;

    while (begin <= input.size()) {
        const std::size_t comma = input.find(',', begin);

        const std::size_t end =
            comma == std::string::npos
                ? input.size()
                : comma;

        const std::string address =
            input.substr(begin, end - begin);

        if (address.empty()) {
            throw std::invalid_argument(
                "server address list contains an empty address"
            );
        }

        if (!unique_addresses.insert(address).second) {
            throw std::invalid_argument(
                "duplicate server address: " + address
            );
        }

        addresses.push_back(address);

        if (comma == std::string::npos) {
            break;
        }

        begin = comma + 1;
    }

    if (addresses.size() > 50) {
        throw std::invalid_argument(
            "server count must not exceed 50"
        );
    }

    return addresses;
}

struct ServerRecord {
    std::string address;
    bool registered = false;
};

class ClusterManagerServiceImpl final
    : public cluster::ClusterManager::Service {
public:
    explicit ClusterManagerServiceImpl(
        std::vector<std::string> addresses
    ) {
        servers_.reserve(addresses.size());

        for (std::string& address : addresses) {
            servers_.push_back(
                ServerRecord{
                    std::move(address),
                    false
                }
            );
        }
    }

    Status RegisterServer(
        ServerContext* /*context*/,
        const cluster::RegisterServerRequest* request,
        cluster::RegisterServerReply* reply
    ) override {
        std::lock_guard<std::mutex> lock(mutex_);

        const std::uint32_t server_id =
            request->server_id();

        if (server_id >= servers_.size()) {
            return Status(
                StatusCode::INVALID_ARGUMENT,
                "server_id is outside the configured range"
            );
        }

        ServerRecord& server = servers_[server_id];

        if (request->address() != server.address) {
            return Status(
                StatusCode::INVALID_ARGUMENT,
                "registration address does not match "
                "the manager configuration"
            );
        }

        const bool first_registration =
            !server.registered;

        server.registered = true;

        FillConfigLocked(reply->mutable_config());

        std::fprintf(
            stderr,
            "%s server %u at %s; cluster ready=%s\n",
            first_registration
                ? "Registered"
                : "Re-registered",
            server_id,
            server.address.c_str(),
            AllRegisteredLocked() ? "true" : "false"
        );

        return Status::OK;
    }

    Status GetCluster(
        ServerContext* /*context*/,
        const cluster::GetClusterRequest* /*request*/,
        cluster::GetClusterReply* reply
    ) override {
        std::lock_guard<std::mutex> lock(mutex_);

        FillConfigLocked(reply->mutable_config());

        return Status::OK;
    }

private:
    bool AllRegisteredLocked() const {
        for (const ServerRecord& server : servers_) {
            if (!server.registered) {
                return false;
            }
        }

        return true;
    }

    void FillConfigLocked(
        cluster::ClusterConfig* config
    ) const {
        config->Clear();

        config->set_server_count(
            static_cast<std::uint32_t>(
                servers_.size()
            )
        );

        config->set_partitioning_scheme(
            cluster::PARTITIONING_SCHEME_FNV1A_64_MOD_N
        );

        config->set_ready(
            AllRegisteredLocked()
        );

        for (
            std::uint32_t server_id = 0;
            server_id < servers_.size();
            ++server_id
        ) {
            const ServerRecord& record =
                servers_[server_id];

            cluster::ServerInfo* output =
                config->add_servers();

            output->set_server_id(server_id);
            output->set_address(record.address);
            output->set_registered(
                record.registered
            );
        }
    }

    std::vector<ServerRecord> servers_;
    mutable std::mutex mutex_;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(
            stderr,
            "Usage: %s <manager_listen_addr> "
            "<server_addr_0,server_addr_1,...>\n",
            argv[0]
        );

        return 1;
    }

    const std::string manager_listen_addr = argv[1];
    const std::string server_addresses_argument = argv[2];

    try {
        std::vector<std::string> server_addresses =
            ParseServerAddresses(
                server_addresses_argument
            );

        ClusterManagerServiceImpl service(
            server_addresses
        );

        ServerBuilder builder;

        builder.AddListeningPort(
            manager_listen_addr,
            grpc::InsecureServerCredentials()
        );

        builder.RegisterService(&service);

        std::unique_ptr<Server> server(
            builder.BuildAndStart()
        );

        if (!server) {
            std::fprintf(
                stderr,
                "Failed to start manager on %s\n",
                manager_listen_addr.c_str()
            );

            return 2;
        }

        std::fprintf(
            stderr,
            "Cluster manager running on %s\n",
            manager_listen_addr.c_str()
        );

        std::fprintf(
            stderr,
            "Configured %zu partition servers:\n",
            server_addresses.size()
        );

        for (
            std::size_t server_id = 0;
            server_id < server_addresses.size();
            ++server_id
        ) {
            std::fprintf(
                stderr,
                "  server %zu: %s\n",
                server_id,
                server_addresses[server_id].c_str()
            );
        }

        server->Wait();
    } catch (const std::exception& error) {
        std::fprintf(
            stderr,
            "Manager failed: %s\n",
            error.what()
        );

        return 3;
    }

    return 0;
}
