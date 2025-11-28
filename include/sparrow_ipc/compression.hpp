#pragma once

#include <cstdint>
#include <optional>
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

    class CompressionCacheImpl;

    class SPARROW_IPC_API CompressionCache
    {
        public:
            CompressionCache();
            ~CompressionCache();

            CompressionCache(CompressionCache&&) noexcept;
            CompressionCache& operator=(CompressionCache&&) noexcept;

            CompressionCache(const CompressionCache&) = delete;
            CompressionCache& operator=(const CompressionCache&) = delete;

            std::optional<std::span<const std::uint8_t>> find(const void* key);
            std::span<const std::uint8_t> store(const void* key, std::vector<std::uint8_t>&& data);

            size_t size() const;
            size_t count(const void* key) const;
            bool empty() const;
            void clear();

        private:
            std::unique_ptr<CompressionCacheImpl> m_pimpl;
    };

    [[nodiscard]] SPARROW_IPC_API std::span<const std::uint8_t> compress(
        const CompressionType compression_type,
        const std::span<const std::uint8_t>& data,
        CompressionCache& cache);

    [[nodiscard]] SPARROW_IPC_API size_t get_compressed_size(
        const CompressionType compression_type,
        const std::span<const std::uint8_t>& data,
        CompressionCache& cache);

    [[nodiscard]] SPARROW_IPC_API std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> decompress(
        const CompressionType compression_type,
        std::span<const std::uint8_t> data);
}
