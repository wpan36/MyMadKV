#include <grpcpp/grpcpp.h>

#include <cctype>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "in_memory_kvstore.grpc.pb.h"
#include "in_memory_kvstore.pb.h"

namespace kv = kvstore_service;

std::vector<std::string> SplitBySpace(const std::string& line){
    std::vector<std::string> words;
    std::string word;
    std::istringstream iss(line);
    while (iss >> word){
        words.emplace_back(word);
    }
    return words;
}

void RpcFail(const std::string& operation, const grpc::Status& status){
    std::cerr << operation << " failed with error code: " << status.error_code()
    << ", message: " << status.error_message() << "\n";

}

int main(int argc, char** argv){
    if (argc != 2){
        std::cerr << "Usage: " << argv[0] << " <server_addr>\n";
        return 1;
    }
    const std::string kServerAddr = argv[1];
    auto channel = grpc::CreateChannel(kServerAddr, grpc::InsecureChannelCredentials());
    std::unique_ptr<kv::Operation::Stub> stub = kv::Operation::NewStub(channel);

    std::string line;
    while (std::getline(std::cin, line)){
        auto words = SplitBySpace(line);
        if (words.empty()) continue;
        
        std::string command = words[0];
        if (command == "PUT"){
            if (words.size() != 3){
                std::cerr << "Invalid PUT\n";
                return 1;
            }
        
            std::string key = words[1];
            std::string value = words[2];

            kv::PutRequest request;
            kv::PutReply reply;
            request.set_key(key);
            request.set_new_value(value);

            grpc::ClientContext context;
            grpc::Status status = stub->Put(&context, request, &reply);
            if (!status.ok()){
                RpcFail("Put", status);
                return 2;
            }

            std::cout << "PUT " << key;
            if (reply.found()){
                std::cout << " found\n";
            }else{
                std::cout << " not_found\n";
            }
            std::cout.flush();

        }else if (command == "SWAP"){
            if (words.size() != 3){
                std::cerr << "Invalid SWAP\n";
                return 1;
            }

            std::string key = words[1];
            std::string value = words[2];

            kv::SwapRequest request;
            kv::SwapReply reply;
            request.set_key(key);
            request.set_new_value(value);

            grpc::ClientContext context;
            grpc::Status status = stub->Swap(&context, request, &reply);
            if (!status.ok()){
                RpcFail("Swap", status);
                return 2;
            }

            std::cout << "SWAP " << key;
            if (reply.found()){
                std::cout << " " << reply.old_value() << "\n";
            }else{
                std::cout << " null\n";
            }
            std::cout.flush();

        }else if (command == "GET"){
            if (words.size() != 2) {
                std::cerr << "Invalid GET\n";
                return 1;
            }

            std::string key = words[1];
            kv::GetRequest request;
            kv::GetReply reply;
            request.set_key(key);

            grpc::ClientContext context;
            grpc::Status status = stub->Get(&context, request, &reply);
            if (!status.ok()){
                RpcFail("Get", status);
                return 2;
            }

            if (reply.found()){
                std::cout << "GET " << key << " " << reply.value() << "\n";
            }else{
                std::cout << "GET " << key << " null\n";
            }
            std::cout.flush();

        }else if (command == "DELETE"){
            if (words.size() != 2) {
                std::cerr << "Invalid DELETE\n";
                return 1;
            }

            std::string key = words[1];
            kv::DeleteRequest request;
            kv::DeleteReply reply;
            request.set_key(key);

            grpc::ClientContext context;
            grpc::Status status = stub->Delete(&context, request, &reply);
            if (!status.ok()){
                RpcFail("Delete", status);
                return 2;
            }

            std::cout << "DELETE " << key;
            if (reply.found()){
                std::cout << " found\n";
            }else{
                std::cout << " not_found\n";
            }
            std::cout.flush();

        }else if (command == "SCAN"){
            if (words.size() != 3) {
                std::cerr << "Invalid SCAN\n";
                return 1;
            }

            std::string start_key = words[1];
            std::string end_key = words[2];
            kv::ScanRequest request;
            kv::ScanReply reply;
            request.set_start_key(start_key);
            request.set_end_key(end_key);

            grpc::ClientContext context;
            grpc::Status status = stub->Scan(&context, request, &reply);
            if (!status.ok()){
                RpcFail("Scan", status);
                return 2;
            }

            std::cout << "SCAN " << start_key << " " << end_key << " BEGIN\n";
            for (const auto& p : reply.list()){
                std::cout << "  " << p.key() << " " << p.value() << "\n";
            }
            std::cout << "SCAN END\n";
            std::cout.flush();

        }else if (command == "STOP"){
            if (words.size() != 1) {
                std::cerr << "Invalid STOP\n";
                return 1;
            }
            std::cout << "STOP\n";
            std::cout.flush();
            break;
        }else{
            std::cerr << "Unknown command: " << command << "\n";
        }
    }
    return 0;
}