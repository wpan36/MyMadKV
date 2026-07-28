#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <grpcpp/grpcpp.h>

#include "in_memory_kvstore.h"
#include "in_memory_kvstore.pb.h"
#include "in_memory_kvstore.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace kv = kvstore_service;

class ServiceImpl final : public kv::Operation::Service{
public:
    explicit ServiceImpl(InMemoryKVStore* store) : store_(store){}
    
    Status Put(ServerContext* /*context*/, const kv::PutRequest* request, kv::PutReply* reply) override{
        std::lock_guard<std::mutex> lk(mu_);
        bool found = store_->Put(request->key(), request->new_value());
        reply->set_found(found);
        return Status::OK;
    }

    Status Swap(ServerContext*, const kv::SwapRequest* request, kv::SwapReply* reply) override{
        std::lock_guard<std::mutex> lk(mu_);
        auto old_value = store_->Swap(request->key(), request->new_value());
        if (old_value.has_value()){
            reply->set_found(true);
            reply->set_old_value(*old_value);
        }else{
            reply->set_found(false);
            reply->clear_old_value();
        }
        return Status::OK;
    }

    Status Get(ServerContext*, const kv::GetRequest* request, kv::GetReply* reply) override{
        std::lock_guard<std::mutex> lk(mu_);
        auto value = store_->Get(request->key());
        reply->set_found(value.has_value());
        if (value.has_value()){
            reply->set_value(*value);
        }else{
            reply->clear_value();
        }
        return Status::OK;
    }

    Status Scan(ServerContext*, const kv::ScanRequest* request, kv::ScanReply* reply) override{
        std::lock_guard<std::mutex> lk(mu_);
        reply->clear_list();
        auto list = store_->Scan(request->start_key(), request->end_key());
        for (const auto& p : list){
            auto* out = reply->add_list();
            out->set_key(p.first);
            out->set_value(p.second);
        }
        return Status::OK;
    }

    Status Delete(ServerContext*, const kv::DeleteRequest* request, kv::DeleteReply* reply) override{
        std::lock_guard<std::mutex> lk(mu_);
        const bool found = store_->Delete(request->key());
        reply->set_found(found);
        return Status::OK;
    }


private:
    InMemoryKVStore* store_;
    std::mutex mu_;
};

int main(int argc, char** argv){
    if (argc != 2){
        std::fprintf(stderr, "Usage: %s <listen_addr>\n", argv[0]);
        return 1;
    }

    const std::string kListenAddr = argv[1];
    InMemoryKVStore store;
    ServiceImpl service(&store);
    ServerBuilder builder;
    builder.AddListeningPort(kListenAddr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server){
        std::fprintf(stderr, "Failed to start server on %s\n", kListenAddr.c_str());
        return 2;
    }

    std::fprintf(stderr, "Server running on %s\n", kListenAddr.c_str());
    server->Wait();
    return 0;
}