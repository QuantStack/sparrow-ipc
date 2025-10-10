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

    std::vector<std::uint8_t> decompress(org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const std::uint8_t> data)
    {
        if (data.empty())
        {
            return {};
        }
        switch (compression_type)
        {
            case org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME:
            {
                if (data.size() < 8)
                {
                    throw std::runtime_error("Invalid compressed data: missing decompressed size");
                }
                const std::int64_t decompressed_size = *reinterpret_cast<const std::int64_t*>(data.data());
                const auto compressed_data = data.subspan(8);

                if (decompressed_size == -1)
                {
                    return {compressed_data.begin(), compressed_data.end()};
                }

                std::vector<std::uint8_t> decompressed_data(decompressed_size);
                LZ4F_dctx* dctx = nullptr;
                LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
                size_t compressed_size_in_out = compressed_data.size();
                size_t decompressed_size_in_out = decompressed_size;
                size_t result = LZ4F_decompress(dctx, decompressed_data.data(), &decompressed_size_in_out, compressed_data.data(), &compressed_size_in_out, nullptr);
                if (LZ4F_isError(result))
                {
                    throw std::runtime_error("Failed to decompress data with LZ4 frame format");
                }
                LZ4F_freeDecompressionContext(dctx);
                return decompressed_data;
            }
            default:
                return {data.begin(), data.end()};
        }
    }
}
