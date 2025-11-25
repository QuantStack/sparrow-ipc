#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <variant>
#include <vector>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    enum class CompressionType
    {
        LZ4_FRAME,
        ZSTD
    };

    using compression_cache_t = std::map<const void*, std::vector<uint8_t>>;

    [[nodiscard]] SPARROW_IPC_API std::span<const uint8_t> compress(
        const CompressionType compression_type,
        const std::span<const uint8_t>& data,
        compression_cache_t& cache);

    // TODO add tests for this
    [[nodiscard]] SPARROW_IPC_API size_t get_compressed_size(
        const CompressionType compression_type,
        const std::span<const uint8_t>& data,
        compression_cache_t& cache);

    [[nodiscard]] SPARROW_IPC_API std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> decompress(
        const CompressionType compression_type,
        std::span<const std::uint8_t> data);
}