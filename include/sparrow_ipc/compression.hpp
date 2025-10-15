#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "Message_generated.h"

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

    std::vector<std::uint8_t> compress(const org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const std::uint8_t> data);
    std::vector<std::uint8_t> decompress(const org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const std::uint8_t> data);
}
