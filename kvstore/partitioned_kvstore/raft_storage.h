#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "raft.pb.h"

namespace madkv::raftcore {

class RaftStorage final {
public:
    using Entry = madkv::raft::LogEntry;

    explicit RaftStorage(const std::string& directory);
    ~RaftStorage();

    RaftStorage(const RaftStorage&) = delete;
    RaftStorage& operator=(const RaftStorage&) = delete;

    std::uint64_t CurrentTerm() const;
    std::optional<std::uint32_t> VotedFor() const;
    std::uint64_t CommitIndex() const;

    void SetCurrentTermAndVote(
        std::uint64_t term,
        std::optional<std::uint32_t> voted_for
    );

    void SetVotedFor(std::optional<std::uint32_t> voted_for);
    void SetCommitIndex(std::uint64_t commit_index);

    std::uint64_t LastLogIndex() const;
    std::uint64_t LastLogTerm() const;

    std::optional<Entry> GetEntry(std::uint64_t index) const;

    std::vector<Entry> GetEntries(
        std::uint64_t start_index,
        std::size_t max_entries
    ) const;

    void AppendEntries(const std::vector<Entry>& entries);
    void TruncateSuffix(std::uint64_t from_index);

private:
    static std::string LogKey(std::uint64_t index);
    static bool HasLogPrefix(const rocksdb::Slice& key);
    static std::uint64_t ParseUnsigned(
        const rocksdb::Slice& value,
        const std::string& field_name
    );

    void LoadMetadata();
    void DiscoverLogTail();

    mutable std::mutex mutex_;
    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;

    std::uint64_t current_term_ = 0;
    std::optional<std::uint32_t> voted_for_;
    std::uint64_t commit_index_ = 0;
    std::uint64_t last_log_index_ = 0;
    std::uint64_t last_log_term_ = 0;
};

}  // namespace madkv::raftcore
