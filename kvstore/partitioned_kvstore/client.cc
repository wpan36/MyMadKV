#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster_client.h"
#include "cluster.pb.h"
#include "in_memory_kvstore.grpc.pb.h"
#include "in_memory_kvstore.pb.h"
#include "partitioning.h"

namespace kv = kvstore_service;
namespace cluster = madkv::cluster;
namespace partitioning = madkv::partitioning;

namespace {

struct ServerConnection {
    std::uint32_t server_id = 0;
    std::string address;

    std::unique_ptr<kv::Operation::Stub> stub;
};

std::vector<std::string> SplitBySpace(
    const std::string& line
) {
    std::vector<std::string> words;
    std::string word;
    std::istringstream input(line);

    while (input >> word) {
        words.push_back(word);
    }

    return words;
}

void RpcFail(
    const std::string& operation,
    const grpc::Status& status
) {
    std::cerr
        << operation
        << " failed with error code: "
        << status.error_code()
        << ", message: "
        << status.error_message()
        << "\n";
}

bool IsRetryable(
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

template <typename Reply, typename Invoke>
bool CallWithRetry(
    const std::string& operation,
    Invoke&& invoke,
    Reply* reply
) {
    using namespace std::chrono_literals;

    auto retry_delay = 100ms;
    constexpr auto max_retry_delay = 1000ms;

    while (true) {
        grpc::ClientContext context;

        context.set_deadline(
            std::chrono::system_clock::now() + 1s
        );

        Reply attempt_reply;

        const grpc::Status status =
            invoke(
                &context,
                &attempt_reply
            );

        if (status.ok()) {
            *reply = std::move(attempt_reply);
            return true;
        }

        if (!IsRetryable(status.error_code())) {
            RpcFail(operation, status);
            return false;
        }

        std::cerr
            << operation
            << " temporarily failed: "
            << status.error_message()
            << "; retrying in "
            << retry_delay.count()
            << " ms\n";

        std::this_thread::sleep_for(retry_delay);

        retry_delay = std::min(
            retry_delay * 2,
            max_retry_delay
        );
    }
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

    const auto now =
        std::chrono::high_resolution_clock::now()
            .time_since_epoch()
            .count();

    std::random_device random_device;

    const std::uint64_t seed =
        static_cast<std::uint64_t>(now) ^
        (
            static_cast<std::uint64_t>(
                random_device()
            ) << 32
        ) ^
        static_cast<std::uint64_t>(
            random_device()
        );

    std::mt19937_64 generator(seed);

    std::ostringstream output;

    output
        << std::hex
        << std::setfill('0')
        << std::setw(16)
        << generator()
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

    try {
        const std::uint64_t result =
            std::stoull(configured);

        if (result == 0) {
            throw std::invalid_argument(
                "request ID must be positive"
            );
        }

        return result;
    } catch (const std::exception&) {
        throw std::runtime_error(
            "MADKV_START_REQUEST_ID must be "
            "a positive integer"
        );
    }
}

void SetRequestHeader(
    kv::RequestHeader* header,
    const std::string& client_id,
    const std::uint64_t request_id
) {
    header->set_client_id(client_id);
    header->set_request_id(request_id);
}

std::vector<ServerConnection> BuildServerConnections(
    const cluster::ClusterConfig& config
) {
    if (!config.ready()) {
        throw std::runtime_error(
            "cluster configuration is not ready"
        );
    }

    if (config.server_count() == 0) {
        throw std::runtime_error(
            "cluster contains zero servers"
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

    if (
        config.servers_size() !=
        static_cast<int>(config.server_count())
    ) {
        throw std::runtime_error(
            "cluster server list size is inconsistent"
        );
    }

    std::vector<ServerConnection> servers(
        config.server_count()
    );

    std::vector<bool> seen(
        config.server_count(),
        false
    );

    for (const cluster::ServerInfo& info :
         config.servers()) {
        if (
            info.server_id() >=
            config.server_count()
        ) {
            throw std::runtime_error(
                "manager returned an out-of-range server_id"
            );
        }

        if (seen[info.server_id()]) {
            throw std::runtime_error(
                "manager returned a duplicate server_id"
            );
        }

        if (!info.registered()) {
            throw std::runtime_error(
                "manager marked cluster ready while a "
                "server was unregistered"
            );
        }

        if (info.address().empty()) {
            throw std::runtime_error(
                "manager returned an empty server address"
            );
        }

        seen[info.server_id()] = true;

        ServerConnection connection;
        connection.server_id = info.server_id();
        connection.address = info.address();

        auto channel = grpc::CreateChannel(
            connection.address,
            grpc::InsecureChannelCredentials()
        );

        connection.stub =
            kv::Operation::NewStub(channel);

        servers[connection.server_id] =
            std::move(connection);
    }

    for (const bool present : seen) {
        if (!present) {
            throw std::runtime_error(
                "manager configuration is missing a server"
            );
        }
    }

    return servers;
}

ServerConnection& ServerForKey(
    std::vector<ServerConnection>& servers,
    const std::string& key
) {
    const std::uint32_t owner =
        partitioning::OwnerForKey(
            key,
            static_cast<std::uint32_t>(
                servers.size()
            )
        );

    return servers.at(owner);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr
            << "Usage: "
            << argv[0]
            << " <manager_addr>\n";

        return 1;
    }

    try {
        const std::string manager_address =
            argv[1];

        ClusterClient cluster_client(
            manager_address
        );

        std::cerr
            << "Waiting for cluster configuration from "
            << manager_address
            << "\n";

        const cluster::ClusterConfig config =
            cluster_client.GetClusterUntilReady();

        std::vector<ServerConnection> servers =
            BuildServerConnections(config);

        std::cerr
            << "Connected to "
            << servers.size()
            << " partition servers\n";

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

            const std::string& command =
                words[0];

            if (command == "PUT") {
                if (words.size() != 3) {
                    std::cerr << "Invalid PUT\n";
                    return 1;
                }

                const std::string& key =
                    words[1];

                const std::string& value =
                    words[2];

                ServerConnection& server =
                    ServerForKey(servers, key);

                kv::PutRequest request;
                kv::PutReply reply;

                request.set_key(key);
                request.set_new_value(value);

                SetRequestHeader(
                    request.mutable_header(),
                    client_id,
                    next_request_id
                );

                const bool succeeded =
                    CallWithRetry<kv::PutReply>(
                        "Put",
                        [&](grpc::ClientContext* context,
                            kv::PutReply* output) {
                            return server.stub->Put(
                                context,
                                request,
                                output
                            );
                        },
                        &reply
                    );

                if (!succeeded) {
                    return 2;
                }

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

                const std::string& key =
                    words[1];

                const std::string& value =
                    words[2];

                ServerConnection& server =
                    ServerForKey(servers, key);

                kv::SwapRequest request;
                kv::SwapReply reply;

                request.set_key(key);
                request.set_new_value(value);

                SetRequestHeader(
                    request.mutable_header(),
                    client_id,
                    next_request_id
                );

                const bool succeeded =
                    CallWithRetry<kv::SwapReply>(
                        "Swap",
                        [&](grpc::ClientContext* context,
                            kv::SwapReply* output) {
                            return server.stub->Swap(
                                context,
                                request,
                                output
                            );
                        },
                        &reply
                    );

                if (!succeeded) {
                    return 2;
                }

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

                const std::string& key =
                    words[1];

                ServerConnection& server =
                    ServerForKey(servers, key);

                kv::GetRequest request;
                kv::GetReply reply;

                request.set_key(key);

                const bool succeeded =
                    CallWithRetry<kv::GetReply>(
                        "Get",
                        [&](grpc::ClientContext* context,
                            kv::GetReply* output) {
                            return server.stub->Get(
                                context,
                                request,
                                output
                            );
                        },
                        &reply
                    );

                if (!succeeded) {
                    return 2;
                }

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

                const std::string& key =
                    words[1];

                ServerConnection& server =
                    ServerForKey(servers, key);

                kv::DeleteRequest request;
                kv::DeleteReply reply;

                request.set_key(key);

                SetRequestHeader(
                    request.mutable_header(),
                    client_id,
                    next_request_id
                );

                const bool succeeded =
                    CallWithRetry<kv::DeleteReply>(
                        "Delete",
                        [&](grpc::ClientContext* context,
                            kv::DeleteReply* output) {
                            return server.stub->Delete(
                                context,
                                request,
                                output
                            );
                        },
                        &reply
                    );

                if (!succeeded) {
                    return 2;
                }

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

                for (ServerConnection& server :
                     servers) {
                    kv::ScanRequest request;
                    kv::ScanReply reply;

                    request.set_start_key(start_key);
                    request.set_end_key(end_key);

                    const std::string operation =
                        "Scan server " +
                        std::to_string(
                            server.server_id
                        );

                    const bool succeeded =
                        CallWithRetry<kv::ScanReply>(
                            operation,
                            [&](grpc::ClientContext* context,
                                kv::ScanReply* output) {
                                return server.stub->Scan(
                                    context,
                                    request,
                                    output
                                );
                            },
                            &reply
                        );

                    if (!succeeded) {
                        return 2;
                    }

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
    } catch (const std::exception& error) {
        std::cerr
            << "Client failed: "
            << error.what()
            << "\n";

        return 3;
    }

    return 0;
}
