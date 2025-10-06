#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "Message_generated.h"

namespace sparrow_ipc
{
//     enum class CompressionType // TODO class ? enum cf. mamba?
//     {
//         NONE,
//         LZ4,
//         ZSTD
//     };

//     CompressionType to_compression_type(org::apache::arrow::flatbuf::CompressionType compression_type);

    std::vector<uint8_t> compress(org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const uint8_t> data);
//     std::vector<uint8_t> decompress(CompressionType type, std::span<const uint8_t> data);

}
