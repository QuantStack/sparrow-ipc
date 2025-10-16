#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "Message_generated.h"

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
// TODO use these later if needed for wrapping purposes (flatbuffers/lz4)
//     enum class CompressionType
//     {
//         NONE,
//         LZ4,
//         ZSTD
//     };

//     CompressionType to_compression_type(org::apache::arrow::flatbuf::CompressionType compression_type);

    SPARROW_IPC_API std::vector<std::uint8_t> compress(const org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const std::uint8_t> data);
    SPARROW_IPC_API std::vector<std::uint8_t> decompress(const org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const std::uint8_t> data);
}
