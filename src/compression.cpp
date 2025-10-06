#include <stdexcept>

#include <lz4frame.h>

#include "sparrow_ipc/compression.hpp"

namespace sparrow_ipc
{
//     CompressionType to_compression_type(org::apache::arrow::flatbuf::CompressionType compression_type)
//     {
//         switch (compression_type)
//         {
//             case org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME:
//                 return CompressionType::LZ4;
// //             case org::apache::arrow::flatbuf::CompressionType::ZSTD:
// //                 // TODO: Add ZSTD support
// //                 break;
//             default:
//                 return CompressionType::NONE;
//         }
//     }

    std::vector<std::uint8_t> compress(org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const std::uint8_t> data)
    {
        if (data.empty())
        {
            return {};
        }
        switch (compression_type)
        {
            case org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME:
            {
                std::int64_t uncompressed_size = data.size();
                const size_t max_compressed_size = LZ4F_compressFrameBound(uncompressed_size, nullptr);
                std::vector<std::uint8_t> compressed_data(max_compressed_size);
                const size_t compressed_size = LZ4F_compressFrame(compressed_data.data(), max_compressed_size, data.data(), uncompressed_size, nullptr);
                if (LZ4F_isError(compressed_size))
                {
                    throw std::runtime_error("Failed to compress data with LZ4 frame format");
                }
                compressed_data.resize(compressed_size);
                return compressed_data;
            }
            default:
                return {data.begin(), data.end()};
        }
    }
}
