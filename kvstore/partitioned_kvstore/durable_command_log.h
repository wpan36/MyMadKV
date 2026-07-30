#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "durable_command.pb.h"

class DurableCommandLog final {
public:
    using Command = madkv::storage::DurableCommand;
    using ReplayCallback = std::function<void(const Command&)>;

    explicit DurableCommandLog(const std::string& directory);
    ~DurableCommandLog();

    DurableCommandLog(const DurableCommandLog&) = delete;
    DurableCommandLog& operator=(const DurableCommandLog&) = delete;

    // Assigns the next sequence number, serializes the command,
    // and synchronously appends it to RocksDB.
    std::uint64_t Append(Command command);

    // Reads all commands in sequence-number order.
    void Replay(const ReplayCallback& callback) const;

    std::uint64_t LastSequence() const noexcept;

private:
    static std::string MakeLogKey(std::uint64_t sequence);
    static bool HasLogPrefix(const rocksdb::Slice& key);

    void DiscoverLastSequence();

    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;
    std::uint64_t last_sequence_ = 0;
};
