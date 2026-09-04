#include "raft_storage.h"

#include <algorithm>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <rocksdb/write_batch.h>

namespace madkv::raftcore {
namespace {

constexpr char kCurrentTermKey[] = "meta/current_term";
constexpr char kVotedForKey[] = "meta/voted_for";
constexpr char kCommitIndexKey[] = "meta/commit_index";

constexpr char kLogPrefix[] = "log/";
constexpr std::size_t kLogPrefixLength = sizeof(kLogPrefix) - 1;
constexpr int kIndexWidth = 20;

void ThrowIfRocksDBError(
    const rocksdb::Status& status,
    const std::string& operation
) {
    if (!status.ok()) {
        throw std::runtime_error(
            operation + " failed: " + status.ToString()
        );
    }
}

std::optional<std::string> ReadOptional(
    rocksdb::DB& db,
    const rocksdb::ReadOptions& options,
    const std::string& key
) {
    std::string value;
    const rocksdb::Status status = db.Get(options, key, &value);

    if (status.IsNotFound()) {
        return std::nullopt;
    }

    ThrowIfRocksDBError(status, "Reading RocksDB key '" + key + "'");
    return value;
}

madkv::raft::LogEntry ParseEntry(const rocksdb::Slice& bytes) {
    madkv::raft::LogEntry entry;

    if (!entry.ParseFromArray(
            bytes.data(),
            static_cast<int>(bytes.size())
        )) {
        throw std::runtime_error(
            "Failed to deserialize a persisted Raft log entry"
        );
    }

    return entry;
}

}  // namespace

RaftStorage::RaftStorage(const std::string& directory) {
    if (directory.empty()) {
        throw std::invalid_argument(
            "Raft storage directory must not be empty"
        );
    }

    std::error_code error;
    std::filesystem::create_directories(directory, error);

    if (error) {
        throw std::runtime_error(
            "Failed to create Raft storage directory '" +
            directory + "': " + error.message()
        );
    }

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* raw_db = nullptr;
    const rocksdb::Status status = rocksdb::DB::Open(
        options,
        directory,
        &raw_db
    );

    ThrowIfRocksDBError(status, "Opening Raft RocksDB");
    db_.reset(raw_db);

    write_options_.sync = true;
    write_options_.disableWAL = false;

    DiscoverLogTail();
    LoadMetadata();

    if (commit_index_ > last_log_index_) {
        throw std::runtime_error(
            "Persisted Raft commit_index is beyond the end of the log"
        );
    }
}

RaftStorage::~RaftStorage() = default;

std::uint64_t RaftStorage::CurrentTerm() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_term_;
}

std::optional<std::uint32_t> RaftStorage::VotedFor() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return voted_for_;
}

std::uint64_t RaftStorage::CommitIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return commit_index_;
}

void RaftStorage::SetCurrentTermAndVote(
    const std::uint64_t term,
    const std::optional<std::uint32_t> voted_for
) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (term < current_term_) {
        throw std::invalid_argument(
            "Raft currentTerm must never decrease"
        );
    }

    rocksdb::WriteBatch batch;
    batch.Put(kCurrentTermKey, std::to_string(term));

    if (voted_for.has_value()) {
        batch.Put(kVotedForKey, std::to_string(*voted_for));
    } else {
        batch.Delete(rocksdb::Slice(kVotedForKey));
    }

    ThrowIfRocksDBError(
        db_->Write(write_options_, &batch),
        "Persisting Raft term/vote"
    );

    current_term_ = term;
    voted_for_ = voted_for;
}

void RaftStorage::SetVotedFor(
    const std::optional<std::uint32_t> voted_for
) {
    std::lock_guard<std::mutex> lock(mutex_);

    rocksdb::Status status;

    if (voted_for.has_value()) {
        status = db_->Put(
            write_options_,
            kVotedForKey,
            std::to_string(*voted_for)
        );
    } else {
        status = db_->Delete(
            write_options_,
            kVotedForKey
        );
    }

    ThrowIfRocksDBError(status, "Persisting Raft votedFor");
    voted_for_ = voted_for;
}

void RaftStorage::SetCommitIndex(const std::uint64_t commit_index) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (commit_index < commit_index_) {
        throw std::invalid_argument(
            "Raft commitIndex must never decrease"
        );
    }

    if (commit_index > last_log_index_) {
        throw std::invalid_argument(
            "Raft commitIndex cannot exceed LastLogIndex"
        );
    }

    ThrowIfRocksDBError(
        db_->Put(
            write_options_,
            kCommitIndexKey,
            std::to_string(commit_index)
        ),
        "Persisting Raft commitIndex"
    );

    commit_index_ = commit_index;
}

std::uint64_t RaftStorage::LastLogIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_log_index_;
}

std::uint64_t RaftStorage::LastLogTerm() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_log_term_;
}

std::optional<RaftStorage::Entry> RaftStorage::GetEntry(
    const std::uint64_t index
) const {
    if (index == 0) {
        return std::nullopt;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    std::string serialized;
    const rocksdb::Status status = db_->Get(
        read_options_,
        LogKey(index),
        &serialized
    );

    if (status.IsNotFound()) {
        return std::nullopt;
    }

    ThrowIfRocksDBError(status, "Reading Raft log entry");

    Entry entry;
    if (!entry.ParseFromString(serialized)) {
        throw std::runtime_error(
            "Failed to deserialize a persisted Raft log entry"
        );
    }

    if (entry.index() != index) {
        throw std::runtime_error(
            "Persisted Raft log key/index mismatch"
        );
    }

    return entry;
}

std::vector<RaftStorage::Entry> RaftStorage::GetEntries(
    const std::uint64_t start_index,
    const std::size_t max_entries
) const {
    if (start_index == 0) {
        throw std::invalid_argument(
            "Raft log indices start at 1"
        );
    }

    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<Entry> entries;

    if (max_entries == 0 || start_index > last_log_index_) {
        return entries;
    }

    entries.reserve(
        std::min<std::size_t>(
            max_entries,
            static_cast<std::size_t>(
                last_log_index_ - start_index + 1
            )
        )
    );

    std::unique_ptr<rocksdb::Iterator> iterator(
        db_->NewIterator(read_options_)
    );

    std::uint64_t expected = start_index;

    for (
        iterator->Seek(LogKey(start_index));
        iterator->Valid() &&
        HasLogPrefix(iterator->key()) &&
        entries.size() < max_entries;
        iterator->Next()
    ) {
        Entry entry = ParseEntry(iterator->value());

        if (entry.index() != expected) {
            throw std::runtime_error(
                "Raft log contains a gap or out-of-order entry"
            );
        }

        entries.push_back(std::move(entry));
        ++expected;
    }

    ThrowIfRocksDBError(
        iterator->status(),
        "Iterating over Raft log entries"
    );

    return entries;
}

void RaftStorage::AppendEntries(
    const std::vector<Entry>& entries
) {
    if (entries.empty()) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    std::uint64_t expected_index = last_log_index_ + 1;
    rocksdb::WriteBatch batch;

    for (const Entry& entry : entries) {
        if (entry.index() != expected_index) {
            throw std::invalid_argument(
                "Raft append must be contiguous and begin at "
                "LastLogIndex + 1"
            );
        }

        if (entry.term() == 0) {
            throw std::invalid_argument(
                "A real Raft log entry cannot have term 0"
            );
        }

        std::string serialized;
        if (!entry.SerializeToString(&serialized)) {
            throw std::runtime_error(
                "Failed to serialize Raft log entry"
            );
        }

        batch.Put(LogKey(entry.index()), serialized);
        ++expected_index;
    }

    ThrowIfRocksDBError(
        db_->Write(write_options_, &batch),
        "Appending Raft log entries"
    );

    last_log_index_ = entries.back().index();
    last_log_term_ = entries.back().term();
}

void RaftStorage::TruncateSuffix(
    const std::uint64_t from_index
) {
    if (from_index == 0) {
        throw std::invalid_argument(
            "Raft log indices start at 1"
        );
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (from_index > last_log_index_) {
        return;
    }

    if (from_index <= commit_index_) {
        throw std::runtime_error(
            "Refusing to truncate a committed Raft log entry"
        );
    }

    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> iterator(
        db_->NewIterator(read_options_)
    );

    for (
        iterator->Seek(LogKey(from_index));
        iterator->Valid() && HasLogPrefix(iterator->key());
        iterator->Next()
    ) {
        batch.Delete(iterator->key());
    }

    ThrowIfRocksDBError(
        iterator->status(),
        "Scanning Raft suffix for truncation"
    );

    ThrowIfRocksDBError(
        db_->Write(write_options_, &batch),
        "Truncating Raft log suffix"
    );

    last_log_index_ = from_index - 1;

    if (last_log_index_ == 0) {
        last_log_term_ = 0;
        return;
    }

    std::string serialized;
    ThrowIfRocksDBError(
        db_->Get(
            read_options_,
            LogKey(last_log_index_),
            &serialized
        ),
        "Reading new Raft log tail after truncation"
    );

    const Entry tail = ParseEntry(serialized);

    if (tail.index() != last_log_index_) {
        throw std::runtime_error(
            "Persisted Raft log key/index mismatch after truncation"
        );
    }

    last_log_term_ = tail.term();
}

std::string RaftStorage::LogKey(const std::uint64_t index) {
    std::ostringstream output;

    output
        << kLogPrefix
        << std::setw(kIndexWidth)
        << std::setfill('0')
        << index;

    return output.str();
}

bool RaftStorage::HasLogPrefix(const rocksdb::Slice& key) {
    return
        key.size() >= kLogPrefixLength &&
        std::memcmp(
            key.data(),
            kLogPrefix,
            kLogPrefixLength
        ) == 0;
}

std::uint64_t RaftStorage::ParseUnsigned(
    const rocksdb::Slice& value,
    const std::string& field_name
) {
    const std::string text(value.data(), value.size());

    if (text.empty()) {
        throw std::runtime_error(
            "Persisted Raft field '" + field_name + "' is empty"
        );
    }

    std::size_t consumed = 0;
    unsigned long long parsed = 0;

    try {
        parsed = std::stoull(text, &consumed, 10);
    } catch (const std::exception&) {
        throw std::runtime_error(
            "Persisted Raft field '" + field_name +
            "' is not an unsigned integer"
        );
    }

    if (consumed != text.size()) {
        throw std::runtime_error(
            "Persisted Raft field '" + field_name +
            "' contains invalid characters"
        );
    }

    return static_cast<std::uint64_t>(parsed);
}

void RaftStorage::LoadMetadata() {
    const auto term = ReadOptional(
        *db_,
        read_options_,
        kCurrentTermKey
    );

    if (term.has_value()) {
        current_term_ = ParseUnsigned(
            rocksdb::Slice(*term),
            "current_term"
        );
    }

    const auto vote = ReadOptional(
        *db_,
        read_options_,
        kVotedForKey
    );

    if (vote.has_value()) {
        const std::uint64_t parsed = ParseUnsigned(
            rocksdb::Slice(*vote),
            "voted_for"
        );

        if (
            parsed >
            std::numeric_limits<std::uint32_t>::max()
        ) {
            throw std::runtime_error(
                "Persisted Raft voted_for is too large"
            );
        }

        voted_for_ = static_cast<std::uint32_t>(parsed);
    }

    const auto commit = ReadOptional(
        *db_,
        read_options_,
        kCommitIndexKey
    );

    if (commit.has_value()) {
        commit_index_ = ParseUnsigned(
            rocksdb::Slice(*commit),
            "commit_index"
        );
    }
}

void RaftStorage::DiscoverLogTail() {
    std::unique_ptr<rocksdb::Iterator> iterator(
        db_->NewIterator(read_options_)
    );

    std::uint64_t expected_index = 1;
    std::uint64_t last_term = 0;

    for (
        iterator->Seek(kLogPrefix);
        iterator->Valid() && HasLogPrefix(iterator->key());
        iterator->Next()
    ) {
        const Entry entry = ParseEntry(iterator->value());

        if (entry.index() != expected_index) {
            throw std::runtime_error(
                "Persisted Raft log is not contiguous from index 1"
            );
        }

        if (entry.term() == 0) {
            throw std::runtime_error(
                "Persisted Raft log contains term 0"
            );
        }

        last_term = entry.term();
        ++expected_index;
    }

    ThrowIfRocksDBError(
        iterator->status(),
        "Inspecting persisted Raft log"
    );

    last_log_index_ = expected_index - 1;
    last_log_term_ = last_term;
}

}  // namespace madkv::raftcore
