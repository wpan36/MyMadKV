#include "in_memory_kvstore.h"

bool 
InMemoryKVStore::Put(const std::string& key, const std::string& new_value){
    bool key_found = false;
    auto it = data_.find(key);
    if (it != data_.end()){
        key_found = true;
    }
    data_[key] = new_value;
    return key_found;
}

std::optional<std::string>
InMemoryKVStore::Swap(const std::string& key, const std::string& new_value) {
    auto it = data_.find(key);

    if (it == data_.end()) {
        data_[key] = new_value;
        return std::nullopt;
    }

    std::string old_value = it->second;
    it->second = new_value;
    return old_value;
}

std::optional<std::string> 
InMemoryKVStore::Get(const std::string& key){
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
}

std::vector<std::pair<std::string, std::string>> 
InMemoryKVStore::Scan(const std::string& start_key, const std::string& end_key){
    std::vector<std::pair<std::string, std::string>> result;
    for (auto it = data_.lower_bound(start_key); it != data_.end() && it->first <= end_key; it++){
        result.emplace_back(it->first, it->second);
    }
    return result;
}

bool 
InMemoryKVStore::Delete(const std::string& key){
    auto it = data_.find(key);
    if (it == data_.end()) return false;
    data_.erase(key);
    return true;
}