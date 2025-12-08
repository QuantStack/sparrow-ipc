#include <cassert>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <tuple>
#include <unordered_map>

#include <lz4frame.h>
#include <zstd.h>

#include "compression_impl.hpp"

namespace sparrow_ipc
{
    struct TupleHasher
    {
        template <class T>
        static inline void hash_combine(std::size_t& seed, const T& v)
        {
            std::hash<T> hasher;
            seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }

        std::size_t operator()(const std::tuple<const void*, size_t>& k) const
        {
            std::size_t seed = 0;
            hash_combine(seed, std::get<0>(k));
            hash_combine(seed, std::get<1>(k));
            return seed;
        }
    };

    class CompressionCacheImpl
    {
        public:
            CompressionCacheImpl() = default;
            ~CompressionCacheImpl() = default;

            CompressionCacheImpl(CompressionCacheImpl&&) noexcept = default;
            CompressionCacheImpl& operator=(CompressionCacheImpl&&) noexcept = default;

            CompressionCacheImpl(const CompressionCacheImpl&) = delete;
            CompressionCacheImpl& operator=(const CompressionCacheImpl&) = delete;

            std::optional<std::span<const std::uint8_t>> find(const void* data_ptr, const size_t data_size)
            {
                auto it = m_cache.find({data_ptr, data_size});
                if (it != m_cache.end())
                {
                    return it->second;
                }
                return std::nullopt;
            }

            std::span<const std::uint8_t> store(const void* data_ptr, const size_t data_size, std::vector<std::uint8_t>&& data)
            {
                auto [it, inserted] = m_cache.emplace(std::piecewise_construct, std::forward_as_tuple(data_ptr, data_size), std::forward_as_tuple(std::move(data)));
                if (!inserted)
                {
                    throw std::runtime_error("Key already exists in compression cache");
                }
                return it->second;
            }

            size_t size() const
            {
                return m_cache.size();
            }

            size_t count(const void* data_ptr, const size_t data_size) const
            {
                return m_cache.count({data_ptr, data_size});
            }

            bool empty() const
            {
                return m_cache.empty();
            }

            void clear()
            {
                m_cache.clear();
            }

        private:
            using cache_key_t = std::tuple<const void*, size_t>;
            using compression_cache_t = std::unordered_map<cache_key_t, std::vector<std::uint8_t>, TupleHasher>;
            compression_cache_t m_cache;
    };

    CompressionCache::CompressionCache() : m_pimpl(std::make_unique<CompressionCacheImpl>()) {}
    CompressionCache::~CompressionCache() = default;

    CompressionCache::CompressionCache(CompressionCache&&) noexcept = default;
    CompressionCache& CompressionCache::operator=(CompressionCache&&) noexcept = default;

    std::optional<std::span<const std::uint8_t>> CompressionCache::find(const void* data_ptr, const size_t data_size)
    {
        return m_pimpl->find(data_ptr, data_size);
    }

    std::span<const std::uint8_t> CompressionCache::store(const void* data_ptr, const size_t data_size, std::vector<std::uint8_t>&& data)
    {
        return m_pimpl->store(data_ptr, data_size, std::move(data));
    }

    size_t CompressionCache::size() const
    {
        return m_pimpl->size();
    }

    size_t CompressionCache::count(const void* data_ptr, const size_t data_size) const
    {
        return m_pimpl->count(data_ptr, data_size);
    }

    bool CompressionCache::empty() const
    {
        return m_pimpl->empty();
    }

    void CompressionCache::clear()
    {
        m_pimpl->clear();
    }

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

        std::vector<std::uint8_t> lz4_compress_with_header(std::span<const std::uint8_t> data)
        {
            const std::int64_t uncompressed_size = data.size();
            const size_t max_compressed_size = LZ4F_compressFrameBound(uncompressed_size, nullptr);
            std::vector<std::uint8_t> result(details::CompressionHeaderSize + max_compressed_size);
            const size_t compressed_size = LZ4F_compressFrame(result.data() + details::CompressionHeaderSize, max_compressed_size, data.data(), uncompressed_size, nullptr);
            if (LZ4F_isError(compressed_size))
            {
                throw std::runtime_error("Failed to compress data with LZ4 frame format");
            }
            memcpy(result.data(), &uncompressed_size, sizeof(uncompressed_size));
            result.resize(details::CompressionHeaderSize + compressed_size);
            return result;
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

        std::vector<std::uint8_t> zstd_compress_with_header(std::span<const std::uint8_t> data)
        {
            const std::int64_t uncompressed_size = data.size();
            const size_t max_compressed_size = ZSTD_compressBound(uncompressed_size);
            std::vector<std::uint8_t> result(details::CompressionHeaderSize + max_compressed_size);
            const size_t compressed_size = ZSTD_compress(result.data() + details::CompressionHeaderSize, max_compressed_size, data.data(), uncompressed_size, 1);
            if (ZSTD_isError(compressed_size))
            {
                throw std::runtime_error("Failed to compress data with ZSTD");
            }
            memcpy(result.data(), &uncompressed_size, sizeof(uncompressed_size));
            result.resize(details::CompressionHeaderSize + compressed_size);
            return result;
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

        void insert_uncompressed_data(std::vector<uint8_t>& result, const std::span<const uint8_t>& data)
        {
            const std::int64_t header = -1;
            result.reserve(details::CompressionHeaderSize + data.size());
            result.insert(result.end(), reinterpret_cast<const uint8_t*>(&header), reinterpret_cast<const uint8_t*>(&header) + sizeof(header));
            result.insert(result.end(), data.begin(), data.end());
        }

        std::span<const std::uint8_t> compress_with_header(
            const std::span<const std::uint8_t>& data,
            compress_func comp_func,
            CompressionCache& cache)
        {
            const void* buffer_ptr = data.data();
            const size_t buffer_size = data.size();

            // Check cache
            if (auto cached_result = cache.find(buffer_ptr, buffer_size))
            {
                return cached_result.value();
            }

            // Not in cache, compress and store
            if (comp_func)
            {
                auto compressed_with_header = comp_func(data);
                // Compression is effective
                if (compressed_with_header.size() - details::CompressionHeaderSize < data.size())
                {
                    return cache.store(buffer_ptr, buffer_size, std::move(compressed_with_header));
                }
            }

            std::vector<uint8_t> result_vec;
            insert_uncompressed_data(result_vec, data);
            return cache.store(buffer_ptr, buffer_size, std::move(result_vec));
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
        const std::span<const std::uint8_t>& data,
        CompressionCache& cache)
    {
        switch (compression_type)
        {
            case CompressionType::LZ4_FRAME:
            {
                return compress_with_header(data, lz4_compress_with_header, cache);
            }
            case CompressionType::ZSTD:
            {
                return compress_with_header(data, zstd_compress_with_header, cache);
            }
        }
        assert(false && "Unhandled compression type");
        return compress_with_header(data, nullptr, cache);
    }

    size_t get_compressed_size(
        const CompressionType compression_type,
        const std::span<const std::uint8_t>& data,
        CompressionCache& cache)
    {
        return compress(compression_type, data, cache).size();
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
