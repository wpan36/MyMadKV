#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster.pb.h"
#include "cluster_client.h"
#include "in_memory_kvstore.grpc.pb.h"
#include "in_memory_kvstore.pb.h"
#include "partitioning.h"

namespace kv = kvstore_service;
namespace cluster = madkv::cluster;
namespace partitioning = madkv::partitioning;

namespace {

struct ReplicaConnection {
    std::uint32_t replica_id = 0;
    std::string address;
    std::unique_ptr<kv::Operation::Stub> stub;
};

struct PartitionConnection {
    std::uint32_t partition_id = 0;
    std::vector<ReplicaConnection> replicas;
    std::size_t preferred_replica = 0;
};

std::vector<std::string> SplitBySpace(
    const std::string& line
) {
    std::vector<std::string> words;
    std::istringstream input(line);
    std::string word;

    while (input >> word) {
        words.push_back(word);
    }

    return words;
}

std::string GenerateClientId() {
    const char* configured =
        std::getenv("MADKV_CLIENT_ID");

    if (
        configured != nullptr &&
        configured[0] != '\0'
    ) {
        return configured;
    }

    std::random_device device;
    std::mt19937_64 generator(
        static_cast<std::uint64_t>(
            std::chrono::high_resolution_clock::now()
                .time_since_epoch()
                .count()
        ) ^
        static_cast<std::uint64_t>(device())
    );

    std::ostringstream output;
    output
        << "client-"
        << std::hex
        << std::setw(16)
        << std::setfill('0')
        << generator()
        << "-"
        << std::setw(16)
        << generator();

    return output.str();
}

std::uint64_t StartingRequestId() {
    const char* configured =
        std::getenv("MADKV_START_REQUEST_ID");

    if (
        configured == nullptr ||
        configured[0] == '\0'
    ) {
        return 1;
    }

    std::size_t consumed = 0;
    unsigned long long parsed = 0;

    try {
        parsed = std::stoull(
            configured,
            &consumed,
            10
        );
    } catch (const std::exception&) {
        throw std::runtime_error(
            "MADKV_START_REQUEST_ID must be a positive integer"
        );
    }

    if (
        consumed != std::string(configured).size() ||
        parsed == 0
    ) {
        throw std::runtime_error(
            "MADKV_START_REQUEST_ID must be a positive integer"
        );
    }

    return static_cast<std::uint64_t>(parsed);
}

void SetRequestHeader(
    kv::RequestHeader* header,
    const std::string& client_id,
    const std::uint64_t request_id
) {
    header->set_client_id(client_id);
    header->set_request_id(request_id);
}

bool IsTransportRetryable(
    const grpc::StatusCode code
) {
    switch (code) {
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

std::optional<std::uint32_t> ParseLeaderHint(
    const grpc::Status& status
) {
    if (
        status.error_code() !=
        grpc::StatusCode::FAILED_PRECONDITION
    ) {
        return std::nullopt;
    }

    constexpr const char* prefix =
        "NOT_LEADER leader_id=";

    const std::string message =
        status.error_message();

    if (message.rfind(prefix, 0) != 0) {
        return std::nullopt;
    }

    const std::string value =
        message.substr(std::string(prefix).size());

    if (value == "unknown") {
        return std::nullopt;
    }

    std::size_t consumed = 0;
    unsigned long parsed = 0;

    try {
        parsed = std::stoul(
            value,
            &consumed,
            10
        );
    } catch (const std::exception&) {
        return std::nullopt;
    }

    if (consumed != value.size()) {
        return std::nullopt;
    }

    return static_cast<std::uint32_t>(parsed);
}

bool IsNotLeaderStatus(
    const grpc::Status& status
) {
    return
        status.error_code() ==
            grpc::StatusCode::FAILED_PRECONDITION &&
        status.error_message().rfind(
            "NOT_LEADER leader_id=",
            0
        ) == 0;
}

std::vector<PartitionConnection>
BuildPartitionConnections(
    const cluster::ClusterConfig& config
) {
    if (!config.ready()) {
        throw std::runtime_error(
            "cluster configuration is not ready"
        );
    }

    if (
        config.partition_count() == 0 ||
        config.server_rf() == 0
    ) {
        throw std::runtime_error(
            "cluster topology is empty"
        );
    }

    if (
        config.partitioning_scheme() !=
        cluster::PARTITIONING_SCHEME_FNV1A_64_MOD_N
    ) {
        throw std::runtime_error(
            "unsupported partitioning scheme"
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
            "cluster replica list size is inconsistent"
        );
    }

    std::vector<PartitionConnection> partitions(
        config.partition_count()
    );

    std::vector<std::vector<bool>> seen(
        config.partition_count(),
        std::vector<bool>(
            config.server_rf(),
            false
        )
    );

    for (
        std::uint32_t partition_id = 0;
        partition_id < config.partition_count();
        ++partition_id
    ) {
        partitions[partition_id].partition_id =
            partition_id;

        partitions[partition_id].replicas.resize(
            config.server_rf()
        );
    }

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

        if (
            seen[info.partition_id()][
                info.replica_id()
            ]
        ) {
            throw std::runtime_error(
                "manager returned duplicate replica coordinates"
            );
        }

        if (!info.registered()) {
            throw std::runtime_error(
                "manager marked cluster ready while a "
                "server replica was unregistered"
            );
        }

        if (info.address().empty()) {
            throw std::runtime_error(
                "manager returned an empty server address"
            );
        }

        seen[info.partition_id()][
            info.replica_id()
        ] = true;

        ReplicaConnection connection;
        connection.replica_id =
            info.replica_id();
        connection.address =
            info.address();

        auto channel = grpc::CreateChannel(
            connection.address,
            grpc::InsecureChannelCredentials()
        );

        connection.stub =
            kv::Operation::NewStub(channel);

        partitions[
            info.partition_id()
        ].replicas[
            info.replica_id()
        ] = std::move(connection);
    }

    for (const auto& partition_seen : seen) {
        for (const bool present : partition_seen) {
            if (!present) {
                throw std::runtime_error(
                    "manager configuration is missing a replica"
                );
            }
        }
    }

    return partitions;
}

PartitionConnection& PartitionForKey(
    std::vector<PartitionConnection>& partitions,
    const std::string& key
) {
    const std::uint32_t owner =
        partitioning::OwnerForKey(
            key,
            static_cast<std::uint32_t>(
                partitions.size()
            )
        );

    return partitions.at(owner);
}

template <typename Reply, typename Invoke>
void CallPartitionWithRetry(
    PartitionConnection& partition,
    const std::string& operation,
    Invoke&& invoke,
    Reply* reply
) {
    using namespace std::chrono_literals;

    if (partition.replicas.empty()) {
        throw std::runtime_error(
            "partition has no replicas"
        );
    }

    auto retry_delay = 50ms;
    constexpr auto max_retry_delay = 1000ms;

    std::size_t failures_in_round = 0;

    while (true) {
        partition.preferred_replica %=
            partition.replicas.size();

        const std::size_t index =
            partition.preferred_replica;

        ReplicaConnection& replica =
            partition.replicas[index];

        grpc::ClientContext context;

        context.set_deadline(
            std::chrono::system_clock::now() + 3s
        );

        Reply attempt_reply;

        const grpc::Status status =
            invoke(
                replica.stub.get(),
                &context,
                &attempt_reply
            );

        if (status.ok()) {
            partition.preferred_replica = index;
            *reply = std::move(attempt_reply);
            return;
        }

        if (IsNotLeaderStatus(status)) {
            const auto leader_hint =
                ParseLeaderHint(status);

            if (
                leader_hint.has_value() &&
                *leader_hint <
                    partition.replicas.size() &&
                *leader_hint != index
            ) {
                partition.preferred_replica =
                    *leader_hint;
            } else {
                partition.preferred_replica =
                    (index + 1) %
                    partition.replicas.size();
            }
        } else if (
            IsTransportRetryable(
                status.error_code()
            )
        ) {
            partition.preferred_replica =
                (index + 1) %
                partition.replicas.size();
        } else {
            throw std::runtime_error(
                operation +
                " to partition " +
                std::to_string(
                    partition.partition_id
                ) +
                " replica " +
                std::to_string(
                    replica.replica_id
                ) +
                " failed permanently with gRPC code " +
                std::to_string(
                    status.error_code()
                ) +
                ": " +
                status.error_message()
            );
        }

        ++failures_in_round;

        if (
            failures_in_round >=
            partition.replicas.size()
        ) {
            std::fprintf(
                stderr,
                "%s on partition %u has no working leader yet; "
                "retrying in %lld ms\n",
                operation.c_str(),
                partition.partition_id,
                static_cast<long long>(
                    retry_delay.count()
                )
            );

            std::this_thread::sleep_for(
                retry_delay
            );

            retry_delay = std::min(
                retry_delay * 2,
                max_retry_delay
            );

            failures_in_round = 0;
        }
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr
            << "Usage: "
            << argv[0]
            << " <manager_addrs>\n";
        return 1;
    }

    try {
        const std::string manager_addresses =
            argv[1];

        ClusterClient cluster_client(
            manager_addresses
        );

        std::cerr
            << "Waiting for cluster configuration from "
            << manager_addresses
            << "\n";

        const cluster::ClusterConfig config =
            cluster_client.GetClusterUntilReady();

        std::vector<PartitionConnection> partitions =
            BuildPartitionConnections(config);

        std::cerr
            << "Connected to "
            << partitions.size()
            << " partitions with server_rf="
            << config.server_rf()
            << "\n";

        const std::string client_id =
            GenerateClientId();

        std::uint64_t next_request_id =
            StartingRequestId();

        std::string line;

        while (std::getline(std::cin, line)) {
            const auto words =
                SplitBySpace(line);

            if (words.empty()) {
                continue;
            }

            const std::string& command = words[0];

            if (command == "PUT") {
                if (words.size() != 3) {
                    std::cerr << "Invalid PUT\n";
                    return 1;
                }

                const std::string& key = words[1];
                const std::string& value = words[2];

                PartitionConnection& partition =
                    PartitionForKey(
                        partitions,
                        key
                    );

                kv::PutRequest request;
                kv::PutReply reply;

                request.set_key(key);
                request.set_new_value(value);

                SetRequestHeader(
                    request.mutable_header(),
                    client_id,
                    next_request_id
                );

                CallPartitionWithRetry<kv::PutReply>(
                    partition,
                    "Put",
                    [&](kv::Operation::Stub* stub,
                        grpc::ClientContext* context,
                        kv::PutReply* output) {
                        return stub->Put(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

                ++next_request_id;

                std::cout << "PUT " << key;

                if (reply.found()) {
                    std::cout << " found\n";
                } else {
                    std::cout << " not_found\n";
                }

                std::cout.flush();
            } else if (command == "SWAP") {
                if (words.size() != 3) {
                    std::cerr << "Invalid SWAP\n";
                    return 1;
                }

                const std::string& key = words[1];
                const std::string& value = words[2];

                PartitionConnection& partition =
                    PartitionForKey(
                        partitions,
                        key
                    );

                kv::SwapRequest request;
                kv::SwapReply reply;

                request.set_key(key);
                request.set_new_value(value);

                SetRequestHeader(
                    request.mutable_header(),
                    client_id,
                    next_request_id
                );

                CallPartitionWithRetry<kv::SwapReply>(
                    partition,
                    "Swap",
                    [&](kv::Operation::Stub* stub,
                        grpc::ClientContext* context,
                        kv::SwapReply* output) {
                        return stub->Swap(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

                ++next_request_id;

                std::cout << "SWAP " << key;

                if (reply.found()) {
                    std::cout
                        << " "
                        << reply.old_value()
                        << "\n";
                } else {
                    std::cout << " null\n";
                }

                std::cout.flush();
            } else if (command == "GET") {
                if (words.size() != 2) {
                    std::cerr << "Invalid GET\n";
                    return 1;
                }

                const std::string& key = words[1];

                PartitionConnection& partition =
                    PartitionForKey(
                        partitions,
                        key
                    );

                kv::GetRequest request;
                kv::GetReply reply;
                request.set_key(key);

                CallPartitionWithRetry<kv::GetReply>(
                    partition,
                    "Get",
                    [&](kv::Operation::Stub* stub,
                        grpc::ClientContext* context,
                        kv::GetReply* output) {
                        return stub->Get(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

                if (reply.found()) {
                    std::cout
                        << "GET "
                        << key
                        << " "
                        << reply.value()
                        << "\n";
                } else {
                    std::cout
                        << "GET "
                        << key
                        << " null\n";
                }

                std::cout.flush();
            } else if (command == "DELETE") {
                if (words.size() != 2) {
                    std::cerr << "Invalid DELETE\n";
                    return 1;
                }

                const std::string& key = words[1];

                PartitionConnection& partition =
                    PartitionForKey(
                        partitions,
                        key
                    );

                kv::DeleteRequest request;
                kv::DeleteReply reply;
                request.set_key(key);

                SetRequestHeader(
                    request.mutable_header(),
                    client_id,
                    next_request_id
                );

                CallPartitionWithRetry<kv::DeleteReply>(
                    partition,
                    "Delete",
                    [&](kv::Operation::Stub* stub,
                        grpc::ClientContext* context,
                        kv::DeleteReply* output) {
                        return stub->Delete(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

                ++next_request_id;

                std::cout << "DELETE " << key;

                if (reply.found()) {
                    std::cout << " found\n";
                } else {
                    std::cout << " not_found\n";
                }

                std::cout.flush();
            } else if (command == "SCAN") {
                if (words.size() != 3) {
                    std::cerr << "Invalid SCAN\n";
                    return 1;
                }

                const std::string& start_key =
                    words[1];
                const std::string& end_key =
                    words[2];

                std::vector<kv::KVPair> merged;

                for (
                    PartitionConnection& partition :
                    partitions
                ) {
                    kv::ScanRequest request;
                    kv::ScanReply reply;

                    request.set_start_key(start_key);
                    request.set_end_key(end_key);

                    CallPartitionWithRetry<kv::ScanReply>(
                        partition,
                        "Scan",
                        [&](kv::Operation::Stub* stub,
                            grpc::ClientContext* context,
                            kv::ScanReply* output) {
                            return stub->Scan(
                                context,
                                request,
                                output
                            );
                        },
                        &reply
                    );

                    for (const kv::KVPair& pair :
                         reply.list()) {
                        merged.push_back(pair);
                    }
                }

                std::sort(
                    merged.begin(),
                    merged.end(),
                    [](
                        const kv::KVPair& left,
                        const kv::KVPair& right
                    ) {
                        return left.key() < right.key();
                    }
                );

                std::cout
                    << "SCAN "
                    << start_key
                    << " "
                    << end_key
                    << " BEGIN\n";

                for (const kv::KVPair& pair :
                     merged) {
                    std::cout
                        << "  "
                        << pair.key()
                        << " "
                        << pair.value()
                        << "\n";
                }

                std::cout << "SCAN END\n";
                std::cout.flush();
            } else if (command == "STOP") {
                if (words.size() != 1) {
                    std::cerr << "Invalid STOP\n";
                    return 1;
                }

                std::cout << "STOP\n";
                std::cout.flush();
                break;
            } else {
                std::cerr
                    << "Unknown command: "
                    << command
                    << "\n";
            }
        }

        return 0;
    } catch (const std::exception& error) {
        std::cerr
            << "Client failed: "
            << error.what()
            << "\n";
        return 3;
    }
}
