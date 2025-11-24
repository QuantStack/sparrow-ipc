#include <cassert>
#include <functional>
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
        using compress_func = std::function<std::vector<uint8_t>(std::span<const uint8_t>)>;
        using decompress_func = std::function<std::vector<uint8_t>(std::span<const uint8_t>, int64_t)>;

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
            const size_t result = LZ4F_decompress(dctx, decompressed_data.data(), &decompressed_size_in_out, data.data(), &compressed_size_in_out, nullptr);
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

        std::span<const std::uint8_t> compress_with_header(
            std::span<const std::uint8_t> data,
            compress_func comp_func,
            bool compression_enabled,
            std::optional<std::reference_wrapper<compression_cache_t>> cache)
        {
            if (!cache)
            {
                throw std::logic_error("compress_with_header with span return type requires a cache.");
            }

            auto& cache_ref = cache->get();
            const void* buffer_ptr = data.data();

            // Check cache
            auto it = cache_ref.find(buffer_ptr);
            if (it != cache_ref.end())
            {
                return it->second;
            }

            // Not in cache, compress and store
            const std::int64_t original_size = data.size();

            std::vector<std::uint8_t> compressed_body;
            if (compression_enabled)
            {
                compressed_body = comp_func(data);
            }

            auto& result_vec_in_cache = cache_ref[buffer_ptr];

            if (compression_enabled && compressed_body.size() < static_cast<size_t>(original_size))
            {
                result_vec_in_cache.reserve(details::CompressionHeaderSize + compressed_body.size());
                result_vec_in_cache.insert(result_vec_in_cache.end(), reinterpret_cast<const uint8_t*>(&original_size), reinterpret_cast<const uint8_t*>(&original_size) + sizeof(original_size));
                result_vec_in_cache.insert(result_vec_in_cache.end(), compressed_body.begin(), compressed_body.end());
            }
            else
            {
                const std::int64_t header = -1;
                result_vec_in_cache.reserve(details::CompressionHeaderSize + data.size());
                result_vec_in_cache.insert(result_vec_in_cache.end(), reinterpret_cast<const uint8_t*>(&header), reinterpret_cast<const uint8_t*>(&header) + sizeof(header));
                result_vec_in_cache.insert(result_vec_in_cache.end(), data.begin(), data.end());
            }

            return result_vec_in_cache;
        }

        size_t get_compressed_size_no_cache(const CompressionType compression_type, std::span<const std::uint8_t> data)
        {
            compress_func comp_func;
            bool compression_enabled = false;

            switch (compression_type)
            {
                case CompressionType::LZ4_FRAME:
                    comp_func = lz4_compress;
                    compression_enabled = true;
                    break;
                case CompressionType::ZSTD:
                    comp_func = zstd_compress;
                    compression_enabled = true;
                    break;
                default:
                    break;
            }

            if (compression_enabled)
            {
                auto compressed_body = comp_func(data);
                if (compressed_body.size() < data.size())
                {
                    return details::CompressionHeaderSize + compressed_body.size();
                }
            }

            return details::CompressionHeaderSize + data.size();
        }

        std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> decompress_with_header(std::span<const std::uint8_t> data, decompress_func decomp_func)
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

            return decomp_func(compressed_data, decompressed_size);
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

    std::span<const std::uint8_t> compress(
        const CompressionType compression_type,
        std::span<const std::uint8_t> data,
        std::optional<std::reference_wrapper<compression_cache_t>> cache)
    {
        switch (compression_type)
        {
            case CompressionType::LZ4_FRAME:
            {
                return compress_with_header(data, lz4_compress, true, cache);
            }
            case CompressionType::ZSTD:
            {
                return compress_with_header(data, zstd_compress, true, cache);
            }
        }
        assert(false && "Unhandled compression type");
        return compress_with_header(data, nullptr, false, cache);
    }

    size_t get_compressed_size(
        const CompressionType compression_type,
        std::span<const std::uint8_t> data,
        std::optional<std::reference_wrapper<compression_cache_t>> cache)
    {
        if (cache)
        {
            return compress(compression_type, data, cache).size();
        }
        return get_compressed_size_no_cache(compression_type, data);
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
                return decompress_with_header(data, lz4_decompress);
            }
            case CompressionType::ZSTD:
            {
                return decompress_with_header(data, zstd_decompress);
            }
            default:
            {
                return get_body_from_uncompressed_data(data);
            }
        }
    }
}
