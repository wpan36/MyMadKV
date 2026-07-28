#pragma once

#include <string>
#include <utility>
#include <optional>
#include <vector>


class KVStore{
public:
    virtual ~KVStore() = default;
    virtual bool Put(const std::string& key, const std::string& new_value) = 0;
    virtual std::optional<std::string> Swap(const std::string& key, const std::string& new_value) = 0;
    virtual std::optional<std::string> Get(const std::string& key) = 0;
    virtual std::vector<std::pair<std::string, std::string>> Scan(const std::string& start_key, 
    const std::string& end_key) = 0;
    virtual bool Delete(const std::string& key) = 0;
};