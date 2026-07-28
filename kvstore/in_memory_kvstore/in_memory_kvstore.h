#pragma once

#include <string>
#include <vector>
#include <optional>
#include <utility>
#include <map>

#include "kvstore.h"

class InMemoryKVStore final: public KVStore{
public:
    InMemoryKVStore() = default;
    ~InMemoryKVStore() override = default;
    bool Put(const std::string& key, const std::string& new_value) override;
    std::optional<std::string> Swap(const std::string& key, const std::string& new_value) override;
    std::optional<std::string> Get(const std::string& key) override;
    std::vector<std::pair<std::string, std::string>> Scan(const std::string& start_key, const std::string& end_key) override;
    bool Delete(const std::string& key) override;

private:
    std::map<std::string, std::string> data_;
};