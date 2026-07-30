#pragma once

#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace madkv::partitioning {

// Stable FNV-1a 64-bit hash.
//
// Do not use std::hash for distributed routing because the C++ standard
// does not guarantee that std::hash produces the same result across
// implementations, library versions, or processes.
inline std::uint64_t Fnv1a64(
    const std::string_view key
) {
    constexpr std::uint64_t kOffsetBasis =
        14695981039346656037ULL;

    constexpr std::uint64_t kPrime =
        1099511628211ULL;

    std::uint64_t hash = kOffsetBasis;

    for (const char character : key) {
        const auto byte =
            static_cast<unsigned char>(character);

        hash ^= static_cast<std::uint64_t>(byte);
        hash *= kPrime;
    }

    return hash;
}

inline std::uint32_t OwnerForKey(
    const std::string_view key,
    const std::uint32_t server_count
) {
    if (server_count == 0) {
        throw std::invalid_argument(
            "server_count must be greater than zero"
        );
    }

    return static_cast<std::uint32_t>(
        Fnv1a64(key) % server_count
    );
}

}  // namespace madkv::partitioning
