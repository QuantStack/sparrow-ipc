#include <stdexcept>

#include <lz4frame.h>
#include <zstd.h>

#include "compression_impl.hpp"

namespace sparrow_ipc
{
    namespace details
    {
        org::apache::arrow::flatbuf::CompressionType to_fb_compression_type(CompressionType compression_type)
        {
            switch (compression_type)
            {
                case CompressionType::LZ4_FRAME:
                    return org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME;
                case CompressionType::ZSTD:
                    return org::apache::arrow::flatbuf::CompressionType::ZSTD;
                default:
                    throw std::invalid_argument("Unsupported compression type.");
            }
        }

        CompressionType from_fb_compression_type(org::apache::arrow::flatbuf::CompressionType compression_type)
        {
            switch (compression_type)
            {
                case org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME:
                    return CompressionType::LZ4_FRAME;
                case org::apache::arrow::flatbuf::CompressionType::ZSTD:
                    return CompressionType::ZSTD;
                default:
                    throw std::invalid_argument("Unsupported compression type.");
            }
        }
    } // namespace details

    namespace
    {
        std::vector<std::uint8_t> lz4_compress(std::span<const std::uint8_t> data)
        {
            const std::int64_t uncompressed_size = data.size();
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

        std::vector<std::uint8_t> lz4_decompress(std::span<const std::uint8_t> data, const std::int64_t decompressed_size)
        {
            std::vector<std::uint8_t> decompressed_data(decompressed_size);
            LZ4F_dctx* dctx = nullptr;
            LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
            size_t compressed_size_in_out = data.size();
            size_t decompressed_size_in_out = decompressed_size;
            size_t result = LZ4F_decompress(dctx, decompressed_data.data(), &decompressed_size_in_out, data.data(), &compressed_size_in_out, nullptr);
            if (LZ4F_isError(result) || (decompressed_size_in_out != (size_t)decompressed_size))
            {
                throw std::runtime_error("Failed to decompress data with LZ4 frame format");
            }
            LZ4F_freeDecompressionContext(dctx);
            return decompressed_data;
        }

        std::vector<std::uint8_t> zstd_compress(std::span<const std::uint8_t> data)
        {
            const std::int64_t uncompressed_size = data.size();
            const size_t max_compressed_size = ZSTD_compressBound(uncompressed_size);
            std::vector<std::uint8_t> compressed_data(max_compressed_size);
            const size_t compressed_size = ZSTD_compress(compressed_data.data(), max_compressed_size, data.data(), uncompressed_size, 1);
            if (ZSTD_isError(compressed_size))
            {
                throw std::runtime_error("Failed to compress data with ZSTD");
            }
            compressed_data.resize(compressed_size);
            return compressed_data;
        }

        std::vector<std::uint8_t> zstd_decompress(std::span<const std::uint8_t> data, const std::int64_t decompressed_size)
        {
            std::vector<std::uint8_t> decompressed_data(decompressed_size);
            const size_t result = ZSTD_decompress(decompressed_data.data(), decompressed_size, data.data(), data.size());
            if (ZSTD_isError(result) || (result != (size_t)decompressed_size))
            {
                throw std::runtime_error("Failed to decompress data with ZSTD");
            }
            return decompressed_data;
        }

        // TODO These functions could be moved to serialize_utils and deserialize_utils if preferred
        // as they are handling the header size
        std::vector<std::uint8_t> uncompressed_data_with_header(std::span<const std::uint8_t> data)
        {
            std::vector<std::uint8_t> result;
            result.reserve(details::CompressionHeaderSize + data.size());
            const std::int64_t header = -1;
            result.insert(result.end(), reinterpret_cast<const uint8_t*>(&header), reinterpret_cast<const uint8_t*>(&header) + sizeof(header));
            result.insert(result.end(), data.begin(), data.end());
            return result;
        }

        std::vector<std::uint8_t> lz4_compress_with_header(std::span<const std::uint8_t> data)
        {
            const std::int64_t original_size = data.size();
            auto compressed_body = lz4_compress(data);

            if (compressed_body.size() >= static_cast<size_t>(original_size))
            {
                return uncompressed_data_with_header(data);
            }

            std::vector<std::uint8_t> result;
            result.reserve(details::CompressionHeaderSize + compressed_body.size());
            result.insert(result.end(), reinterpret_cast<const uint8_t*>(&original_size), reinterpret_cast<const uint8_t*>(&original_size) + sizeof(original_size));
            result.insert(result.end(), compressed_body.begin(), compressed_body.end());
            return result;
        }

        std::vector<std::uint8_t> zstd_compress_with_header(std::span<const std::uint8_t> data)
        {
            const std::int64_t original_size = data.size();
            auto compressed_body = zstd_compress(data);

            if (compressed_body.size() >= static_cast<size_t>(original_size))
            {
                return uncompressed_data_with_header(data);
            }

            std::vector<std::uint8_t> result;
            result.reserve(details::CompressionHeaderSize + compressed_body.size());
            result.insert(result.end(), reinterpret_cast<const uint8_t*>(&original_size), reinterpret_cast<const uint8_t*>(&original_size) + sizeof(original_size));
            result.insert(result.end(), compressed_body.begin(), compressed_body.end());
            return result;
        }

        std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> lz4_decompress_with_header(std::span<const std::uint8_t> data)
        {
            if (data.size() < details::CompressionHeaderSize)
            {
                throw std::runtime_error("Invalid compressed data: missing decompressed size");
            }
            const std::int64_t decompressed_size = *reinterpret_cast<const std::int64_t*>(data.data());
            const auto compressed_data = data.subspan(details::CompressionHeaderSize);

            if (decompressed_size == -1)
            {
                return compressed_data;
            }

            return lz4_decompress(compressed_data, decompressed_size);
        }

        std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> zstd_decompress_with_header(std::span<const std::uint8_t> data)
        {
            if (data.size() < details::CompressionHeaderSize)
            {
                throw std::runtime_error("Invalid compressed data: missing decompressed size");
            }
            const std::int64_t decompressed_size = *reinterpret_cast<const std::int64_t*>(data.data());
            const auto compressed_data = data.subspan(details::CompressionHeaderSize);

            if (decompressed_size == -1)
            {
                return compressed_data;
            }

            return zstd_decompress(compressed_data, decompressed_size);
        }

        std::span<const uint8_t> get_body_from_uncompressed_data(std::span<const uint8_t> data)
        {
            if (data.size() < details::CompressionHeaderSize)
            {
                throw std::runtime_error("Invalid data: missing header");
            }
            return data.subspan(details::CompressionHeaderSize);
        }
    } // namespace

    std::vector<std::uint8_t> compress(const CompressionType compression_type, std::span<const std::uint8_t> data)
    {
        switch (compression_type)
        {
            case CompressionType::LZ4_FRAME:
            {
                return lz4_compress_with_header(data);
            }
            case CompressionType::ZSTD:
            {
                return zstd_compress_with_header(data);
            }
            default:
                return uncompressed_data_with_header(data);
        }
    }

    std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> decompress(const CompressionType compression_type, std::span<const std::uint8_t> data)
    {
        if (data.empty())
        {
            throw std::runtime_error("Trying to decompress empty data.");
        }

        switch (compression_type)
        {
            case CompressionType::LZ4_FRAME:
            {
                return lz4_decompress_with_header(data);
            }
            case CompressionType::ZSTD:
            {
                return zstd_decompress_with_header(data);
            }
            default:
            {
                return get_body_from_uncompressed_data(data);
            }
        }
    }
}
