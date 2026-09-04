#include <cstdint>
#include <cstdio>
#include <exception>
#include <limits>
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

std::vector<std::string> ParseAddresses(
    const std::string& input
) {
    if (input.empty()) {
        throw std::invalid_argument(
            "server address list must not be empty"
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
                "server address list contains an empty address"
            );
        }

        if (!seen.insert(address).second) {
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

    return addresses;
}

struct ReplicaRecord {
    std::string address;
    bool registered = false;
};

class ClusterManagerService final
    : public cluster::ClusterManager::Service {
public:
    ClusterManagerService(
        const std::uint32_t server_rf,
        std::vector<std::string> addresses
    )
        : server_rf_(server_rf) {
        if (
            server_rf_ == 0 ||
            server_rf_ > 9 ||
            (server_rf_ % 2) == 0
        ) {
            throw std::invalid_argument(
                "server_rf must be an odd number from 1 to 9"
            );
        }

        if (
            addresses.empty() ||
            addresses.size() % server_rf_ != 0
        ) {
            throw std::invalid_argument(
                "server address count must be a positive "
                "multiple of server_rf"
            );
        }

        partition_count_ =
            static_cast<std::uint32_t>(
                addresses.size() / server_rf_
            );

        replicas_.reserve(addresses.size());

        for (std::string& address : addresses) {
            replicas_.push_back(
                ReplicaRecord{
                    std::move(address),
                    false
                }
            );
        }
    }

    grpc::Status RegisterServer(
        grpc::ServerContext* /*context*/,
        const cluster::RegisterServerRequest* request,
        cluster::RegisterServerReply* reply
    ) override {
        std::lock_guard<std::mutex> lock(mutex_);

        if (
            request->partition_id() >= partition_count_ ||
            request->replica_id() >= server_rf_
        ) {
            return grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "partition_id/replica_id is outside topology"
            );
        }

        const std::size_t flat_index =
            static_cast<std::size_t>(
                request->partition_id()
            ) *
            server_rf_ +
            request->replica_id();

        ReplicaRecord& record =
            replicas_.at(flat_index);

        if (request->address() != record.address) {
            return grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "registration address does not match "
                "the configured server address"
            );
        }

        const bool first_registration =
            !record.registered;

        record.registered = true;

        FillConfigLocked(reply->mutable_config());

        std::fprintf(
            stderr,
            "%s partition %u replica %u at %s; ready=%s\n",
            first_registration
                ? "Registered"
                : "Re-registered",
            request->partition_id(),
            request->replica_id(),
            record.address.c_str(),
            AllRegisteredLocked() ? "true" : "false"
        );

        return grpc::Status::OK;
    }

    grpc::Status GetCluster(
        grpc::ServerContext* /*context*/,
        const cluster::GetClusterRequest* /*request*/,
        cluster::GetClusterReply* reply
    ) override {
        std::lock_guard<std::mutex> lock(mutex_);
        FillConfigLocked(reply->mutable_config());
        return grpc::Status::OK;
    }

private:
    bool AllRegisteredLocked() const {
        for (const ReplicaRecord& replica : replicas_) {
            if (!replica.registered) {
                return false;
            }
        }

        return true;
    }

    void FillConfigLocked(
        cluster::ClusterConfig* config
    ) const {
        config->Clear();

        config->set_partition_count(partition_count_);
        config->set_server_rf(server_rf_);
        config->set_partitioning_scheme(
            cluster::PARTITIONING_SCHEME_FNV1A_64_MOD_N
        );
        config->set_ready(AllRegisteredLocked());

        for (
            std::uint32_t partition_id = 0;
            partition_id < partition_count_;
            ++partition_id
        ) {
            for (
                std::uint32_t replica_id = 0;
                replica_id < server_rf_;
                ++replica_id
            ) {
                const std::size_t flat_index =
                    static_cast<std::size_t>(
                        partition_id
                    ) *
                    server_rf_ +
                    replica_id;

                const ReplicaRecord& record =
                    replicas_[flat_index];

                cluster::ReplicaInfo* output =
                    config->add_replicas();

                output->set_partition_id(partition_id);
                output->set_replica_id(replica_id);
                output->set_address(record.address);
                output->set_registered(
                    record.registered
                );
            }
        }
    }

    const std::uint32_t server_rf_;
    std::uint32_t partition_count_ = 0;
    std::vector<ReplicaRecord> replicas_;
    mutable std::mutex mutex_;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 8) {
        std::fprintf(
            stderr,
            "Usage: %s <replica_id> <manager_listen_addr> "
            "<p2p_listen_addr> <peer_addrs> <server_rf> "
            "<server_addrs> <backer_path>\n",
            argv[0]
        );
        return 1;
    }

    try {
        const std::uint32_t replica_id =
            ParseUint32(argv[1], "replica_id");

        if (replica_id != 0) {
            throw std::invalid_argument(
                "manager replication is not implemented; "
                "mandatory Project 3 uses only manager replica 0"
            );
        }

        const std::string manager_listen = argv[2];

        // These parameters remain accepted so the mandatory single manager
        // is compatible with the official P3 recipe signature.
        const std::string p2p_listen = argv[3];
        const std::string peer_addresses = argv[4];
        const std::uint32_t server_rf =
            ParseUint32(argv[5], "server_rf");

        std::vector<std::string> server_addresses =
            ParseAddresses(argv[6]);

        const std::string backer_path = argv[7];

        ClusterManagerService service(
            server_rf,
            server_addresses
        );

        grpc::ServerBuilder builder;
        builder.AddListeningPort(
            manager_listen,
            grpc::InsecureServerCredentials()
        );
        builder.RegisterService(&service);

        std::unique_ptr<grpc::Server> server(
            builder.BuildAndStart()
        );

        if (!server) {
            throw std::runtime_error(
                "failed to start cluster manager on " +
                manager_listen
            );
        }

        std::fprintf(
            stderr,
            "Mandatory single manager running on %s\n"
            "Configured partitions=%zu server_rf=%u replicas=%zu\n"
            "Ignored manager-replication parameters: "
            "p2p=%s peers=%s backer=%s\n",
            manager_listen.c_str(),
            server_addresses.size() / server_rf,
            server_rf,
            server_addresses.size(),
            p2p_listen.c_str(),
            peer_addresses.c_str(),
            backer_path.c_str()
        );

        server->Wait();
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(
            stderr,
            "Manager failed: %s\n",
            error.what()
        );
        return 2;
    }
}
