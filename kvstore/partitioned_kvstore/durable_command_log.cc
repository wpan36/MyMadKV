#include "durable_command_log.h"

#include <filesystem>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>

namespace {

constexpr char kLogPrefix[] = "log/";
constexpr std::size_t kLogPrefixLength = sizeof(kLogPrefix) - 1;
constexpr int kSequenceWidth = 20;

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

madkv::storage::DurableCommand ParseCommand(
    const rocksdb::Slice& bytes
) {
    madkv::storage::DurableCommand command;

    if (!command.ParseFromArray(
            bytes.data(),
            static_cast<int>(bytes.size())
        )) {
        throw std::runtime_error(
            "Failed to deserialize durable command"
        );
    }

    return command;
}

}  // namespace

DurableCommandLog::DurableCommandLog(
    const std::string& directory
) {
    if (directory.empty()) {
        throw std::invalid_argument(
            "Durable log directory must not be empty"
        );
    }

    std::error_code error;
    std::filesystem::create_directories(directory, error);

    if (error) {
        throw std::runtime_error(
            "Failed to create durable log directory '" +
            directory +
            "': " +
            error.message()
        );
    }

    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::DB* raw_db = nullptr;

    const rocksdb::Status status =
        rocksdb::DB::Open(
            options,
            directory,
            &raw_db
        );

    ThrowIfRocksDBError(
        status,
        "Opening RocksDB command log"
    );

    db_.reset(raw_db);

    // Do not acknowledge an operation until its WAL record has
    // been synchronously flushed to durable storage.
    write_options_.sync = true;
    write_options_.disableWAL = false;

    DiscoverLastSequence();
}

DurableCommandLog::~DurableCommandLog() = default;

std::uint64_t DurableCommandLog::Append(
    Command command
) {
    const std::uint64_t next_sequence =
        last_sequence_ + 1;

    command.set_sequence(next_sequence);

    std::string serialized;

    if (!command.SerializeToString(&serialized)) {
        throw std::runtime_error(
            "Failed to serialize durable command"
        );
    }

    const rocksdb::Status status =
        db_->Put(
            write_options_,
            MakeLogKey(next_sequence),
            serialized
        );

    ThrowIfRocksDBError(
        status,
        "Appending durable command"
    );

    last_sequence_ = next_sequence;
    return next_sequence;
}

void DurableCommandLog::Replay(
    const ReplayCallback& callback
) const {
    if (!callback) {
        throw std::invalid_argument(
            "Replay callback must not be empty"
        );
    }

    std::unique_ptr<rocksdb::Iterator> iterator(
        db_->NewIterator(read_options_)
    );

    for (
        iterator->Seek(kLogPrefix);
        iterator->Valid() && HasLogPrefix(iterator->key());
        iterator->Next()
    ) {
        const Command command =
            ParseCommand(iterator->value());

        callback(command);
    }

    ThrowIfRocksDBError(
        iterator->status(),
        "Reading durable command log"
    );
}

std::uint64_t DurableCommandLog::LastSequence() const noexcept {
    return last_sequence_;
}

std::string DurableCommandLog::MakeLogKey(
    const std::uint64_t sequence
) {
    std::ostringstream output;

    output
        << kLogPrefix
        << std::setw(kSequenceWidth)
        << std::setfill('0')
        << sequence;

    return output.str();
}

bool DurableCommandLog::HasLogPrefix(
    const rocksdb::Slice& key
) {
    return
        key.size() >= kLogPrefixLength &&
        std::memcmp(
            key.data(),
            kLogPrefix,
            kLogPrefixLength
        ) == 0;
}

void DurableCommandLog::DiscoverLastSequence() {
    std::unique_ptr<rocksdb::Iterator> iterator(
        db_->NewIterator(read_options_)
    );

    std::uint64_t previous_sequence = 0;

    for (
        iterator->Seek(kLogPrefix);
        iterator->Valid() && HasLogPrefix(iterator->key());
        iterator->Next()
    ) {
        const Command command =
            ParseCommand(iterator->value());

        if (command.sequence() <= previous_sequence) {
            throw std::runtime_error(
                "Durable command log contains invalid "
                "or non-increasing sequence numbers"
            );
        }

        previous_sequence = command.sequence();
    }

    ThrowIfRocksDBError(
        iterator->status(),
        "Inspecting durable command log"
    );

    last_sequence_ = previous_sequence;
}
