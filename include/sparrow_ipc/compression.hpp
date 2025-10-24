#pragma once

#include <cstdint>
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

    [[nodiscard]] SPARROW_IPC_API std::vector<std::uint8_t> compress(
        const CompressionType compression_type,
        std::span<const std::uint8_t> data);

    [[nodiscard]] SPARROW_IPC_API std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> decompress(
        const CompressionType compression_type,
        std::span<const std::uint8_t> data);
}
