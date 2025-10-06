#include <iostream>
#include <lz4frame.h>
#include <stdexcept>
#include <vector>

#include "sparrow_ipc/compression.hpp"

namespace sparrow_ipc
{
    // TODO not sure we need this unless if we need it to hide flatbuffers dependency
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

    std::vector<uint8_t> compress(org::apache::arrow::flatbuf::CompressionType compression_type, std::span<const uint8_t> data)
    {
        if (data.empty())
        {
            return {};
        }
        switch (compression_type)
        {
            case org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME:
            {
                int64_t uncompressed_size = data.size();
                const size_t max_compressed_size = LZ4F_compressFrameBound(uncompressed_size, nullptr);
                std::vector<uint8_t> compressed_data(max_compressed_size);
                const size_t compressed_size = LZ4F_compressFrame(compressed_data.data(), max_compressed_size, data.data(), uncompressed_size, nullptr);
                if (LZ4F_isError(compressed_size))
                {
                    throw std::runtime_error("Failed to compress data with LZ4 frame format");
                }
                compressed_data.resize(compressed_size);
                return compressed_data;
            }
//             case CompressionType::NONE:
            default:
                return {data.begin(), data.end()};
        }
    }

//     std::vector<uint8_t> decompress(CompressionType type, std::span<const uint8_t> data)
//     {
//         switch (type)
//         {
//         case CompressionType::LZ4:
//         {
//             if (data.empty())
//             {
//                 return {};
//             }
//             if (data.size() < sizeof(int64_t))
//             {
//                 throw std::runtime_error("Invalid LZ4 compressed data: missing uncompressed size");
//             }
//             const int64_t uncompressed_size = *reinterpret_cast<const int64_t *>(data.data());
//             if (uncompressed_size == -1)
//             {
//                 return {data.begin() + sizeof(uncompressed_size), data.end()};
//             }
// 
//             std::vector<uint8_t> decompressed_data(uncompressed_size);
//             LZ4F_dctx *dctx;
//             if (LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION)))
//             {
//                 throw std::runtime_error("Failed to create LZ4 decompression context");
//             }
// 
//             size_t decompressed_size = uncompressed_size;
//             size_t src_size = data.size() - sizeof(uncompressed_size);
//             const size_t result = LZ4F_decompress(dctx, decompressed_data.data(), &decompressed_size, data.data() + sizeof(uncompressed_size), &src_size, nullptr);
// 
//             LZ4F_freeDecompressionContext(dctx);
// 
//             if (LZ4F_isError(result) || decompressed_size != (size_t)uncompressed_size)
//             {
//                 throw std::runtime_error("Failed to decompress data with LZ4 frame format");
//             }
// 
//             return decompressed_data;
//         }
//         case CompressionType::NONE:
//         default:
//             return {data.begin(), data.end()};
//         }
//     }

}
