#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "in_memory_kvstore.grpc.pb.h"

namespace {

namespace kv = kvstore_service;

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

void SetHeader(
    kv::RequestHeader* header,
    const std::string& client_id,
    const std::uint64_t request_id
) {
    header->set_client_id(client_id);
    header->set_request_id(request_id);
}

template <typename Reply, typename Invoke>
grpc::Status CallOnce(
    Invoke&& invoke,
    Reply* reply
) {
    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds(8)
    );

    return invoke(&context, reply);
}

void PrintRpcError(const grpc::Status& status) {
    std::cout
        << "RPC_ERROR code="
        << status.error_code()
        << " message='"
        << status.error_message()
        << "'\n";
    std::cout.flush();
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2 && argc != 3) {
        std::cerr
            << "Usage: "
            << argv[0]
            << " <server_addr> [client_id]\n";
        return 1;
    }

    const std::string server_address = argv[1];

    const std::string client_id =
        argc == 3
            ? argv[2]
            : "step4-direct-client";

    auto channel = grpc::CreateChannel(
        server_address,
        grpc::InsecureChannelCredentials()
    );

    auto stub = kv::Operation::NewStub(channel);

    std::uint64_t next_request_id = 1;
    std::string line;

    while (std::getline(std::cin, line)) {
        const auto words = SplitBySpace(line);

        if (words.empty()) {
            continue;
        }

        const std::string& command = words[0];

        if (command == "STOP") {
            std::cout << "STOP\n";
            std::cout.flush();
            break;
        }

        if (command == "PUT") {
            if (words.size() != 3) {
                std::cout << "INVALID PUT\n";
                continue;
            }

            kv::PutRequest request;
            kv::PutReply reply;

            request.set_key(words[1]);
            request.set_new_value(words[2]);

            SetHeader(
                request.mutable_header(),
                client_id,
                next_request_id++
            );

            const grpc::Status status =
                CallOnce<kv::PutReply>(
                    [&](grpc::ClientContext* context,
                        kv::PutReply* output) {
                        return stub->Put(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

            if (!status.ok()) {
                PrintRpcError(status);
                continue;
            }

            std::cout
                << "PUT "
                << words[1]
                << (reply.found()
                    ? " found\n"
                    : " not_found\n");

            std::cout.flush();
            continue;
        }

        if (command == "SWAP") {
            if (words.size() != 3) {
                std::cout << "INVALID SWAP\n";
                continue;
            }

            kv::SwapRequest request;
            kv::SwapReply reply;

            request.set_key(words[1]);
            request.set_new_value(words[2]);

            SetHeader(
                request.mutable_header(),
                client_id,
                next_request_id++
            );

            const grpc::Status status =
                CallOnce<kv::SwapReply>(
                    [&](grpc::ClientContext* context,
                        kv::SwapReply* output) {
                        return stub->Swap(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

            if (!status.ok()) {
                PrintRpcError(status);
                continue;
            }

            std::cout
                << "SWAP "
                << words[1];

            if (reply.found()) {
                std::cout << " " << reply.old_value();
            } else {
                std::cout << " null";
            }

            std::cout << "\n";
            std::cout.flush();
            continue;
        }

        if (command == "GET") {
            if (words.size() != 2) {
                std::cout << "INVALID GET\n";
                continue;
            }

            kv::GetRequest request;
            kv::GetReply reply;
            request.set_key(words[1]);

            const grpc::Status status =
                CallOnce<kv::GetReply>(
                    [&](grpc::ClientContext* context,
                        kv::GetReply* output) {
                        return stub->Get(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

            if (!status.ok()) {
                PrintRpcError(status);
                continue;
            }

            std::cout
                << "GET "
                << words[1]
                << " ";

            if (reply.found()) {
                std::cout << reply.value();
            } else {
                std::cout << "null";
            }

            std::cout << "\n";
            std::cout.flush();
            continue;
        }

        if (command == "DELETE") {
            if (words.size() != 2) {
                std::cout << "INVALID DELETE\n";
                continue;
            }

            kv::DeleteRequest request;
            kv::DeleteReply reply;
            request.set_key(words[1]);

            SetHeader(
                request.mutable_header(),
                client_id,
                next_request_id++
            );

            const grpc::Status status =
                CallOnce<kv::DeleteReply>(
                    [&](grpc::ClientContext* context,
                        kv::DeleteReply* output) {
                        return stub->Delete(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

            if (!status.ok()) {
                PrintRpcError(status);
                continue;
            }

            std::cout
                << "DELETE "
                << words[1]
                << (reply.found()
                    ? " found\n"
                    : " not_found\n");

            std::cout.flush();
            continue;
        }

        if (command == "SCAN") {
            if (words.size() != 3) {
                std::cout << "INVALID SCAN\n";
                continue;
            }

            kv::ScanRequest request;
            kv::ScanReply reply;

            request.set_start_key(words[1]);
            request.set_end_key(words[2]);

            const grpc::Status status =
                CallOnce<kv::ScanReply>(
                    [&](grpc::ClientContext* context,
                        kv::ScanReply* output) {
                        return stub->Scan(
                            context,
                            request,
                            output
                        );
                    },
                    &reply
                );

            if (!status.ok()) {
                PrintRpcError(status);
                continue;
            }

            std::cout
                << "SCAN "
                << words[1]
                << " "
                << words[2]
                << " BEGIN\n";

            for (const auto& pair : reply.list()) {
                std::cout
                    << "  "
                    << pair.key()
                    << " "
                    << pair.value()
                    << "\n";
            }

            std::cout << "SCAN END\n";
            std::cout.flush();
            continue;
        }

        std::cout
            << "UNKNOWN_COMMAND "
            << command
            << "\n";
        std::cout.flush();
    }

    return 0;
}
