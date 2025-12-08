#pragma once

#include <cstdint>
#include <functional>
#include <numeric>
#include <ranges>
#include <vector>

namespace sparrow_ipc
{
    /**
     * @brief An output stream that writes data into separate memory chunks.
     *
     * This template class stores data in discrete memory chunks
     * rather than a single contiguous buffer. Each write operation creates a new chunk, making it
     * suitable for scenarios where data needs to be processed or transmitted in separate units.
     *
     * @tparam R A random access range type where each element is itself a random access range of uint8_t.
     *           Typically std::vector<std::vector<uint8_t>> or similar nested container types.
     *
     * @details The chunked approach offers several benefits:
     * - Avoids large contiguous memory allocations
     * - Enables efficient chunk-by-chunk processing or transmission
     * - Supports memory reservation for the chunk container (not individual chunks)
     *
     * @note Each write operation creates a new chunk in the container, regardless of the write size.
     */
    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    class chunked_memory_output_stream
    {
    public:

        /**
         * @brief Constructs a chunked memory output stream with a reference to a chunk container.
         *
         * @param chunks Reference to the container that will store the memory chunks.
         *               The stream stores a pointer to this container for write operations.
         */
        explicit chunked_memory_output_stream(R& chunks)
            : m_chunks(&chunks) {};

        /**
         * @brief Writes character data as a new chunk.
         *
         * Creates a new chunk containing the specified character data.
         *
         * @param s Pointer to the character data to write
         * @param count Number of characters to write
         * @return Reference to this stream for method chaining
         */
        chunked_memory_output_stream<R>& write(const char* s, std::streamsize count);

        /**
         * @brief Writes a span of bytes as a new chunk.
         *
         * Creates a new chunk containing the data from the provided span.
         *
         * @param span A span of bytes to write as a new chunk
         * @return Reference to this stream for method chaining
         */
        chunked_memory_output_stream<R>& write(std::span<const std::uint8_t> span);

        /**
         * @brief Writes a buffer by moving it into the chunk container.
         *
         * This is an optimized write operation that moves an existing buffer into the chunk
         * container, avoiding a copy operation.
         *
         * @param buffer A vector of bytes to move into the chunk container
         * @return Reference to this stream for method chaining
         */
        chunked_memory_output_stream<R>& write(std::vector<uint8_t>&& buffer);

        /**
         * @brief Writes a byte value repeated a specified number of times as a new chunk.
         *
         * Creates a new chunk filled with the specified byte value.
         *
         * @param value The byte value to write
         * @param count Number of times to repeat the value
         * @return Reference to this stream for method chaining
         */
        chunked_memory_output_stream<R>& write(uint8_t value, std::size_t count);

        /**
         * @brief Writes a single character as a new chunk.
         *
         * Creates a new chunk containing a single byte.
         *
         * @param value The character value to write
         * @return Reference to this stream for method chaining
         */
        chunked_memory_output_stream<R>& put(char value);

        /**
         * @brief Reserves capacity in the chunk container.
         *
         * Reserves space for the specified number of chunks in the container.
         * This does not reserve space within individual chunks.
         *
         * @param size Number of chunks to reserve space for
         */
        void reserve(std::size_t size);

        /**
         * @brief Reserves capacity using a lazy calculation function.
         *
         * Reserves space for chunks by calling the provided function to determine the count.
         *
         * @param calculate_reserve_size Function that returns the number of chunks to reserve
         */
        void reserve(const std::function<std::size_t()>& calculate_reserve_size);

        /**
         * @brief Gets the total size of all chunks.
         *
         * Calculates and returns the sum of sizes of all chunks in the container.
         *
         * @return The total number of bytes across all chunks
         */
        [[nodiscard]] size_t size() const;

    private:

        R* m_chunks;
    };

    // Implementation

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    chunked_memory_output_stream<R>& chunked_memory_output_stream<R>::write(const char* s, std::streamsize count)
    {
        m_chunks->emplace_back(s, s + count);
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    chunked_memory_output_stream<R>& chunked_memory_output_stream<R>::write(std::span<const std::uint8_t> span)
    {
        m_chunks->emplace_back(span.begin(), span.end());
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    chunked_memory_output_stream<R>& chunked_memory_output_stream<R>::write(std::vector<uint8_t>&& buffer)
    {
        m_chunks->emplace_back(std::move(buffer));
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    chunked_memory_output_stream<R>& chunked_memory_output_stream<R>::write(uint8_t value, std::size_t count)
    {
        m_chunks->emplace_back(count, value);
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    chunked_memory_output_stream<R>& chunked_memory_output_stream<R>::put(char value)
    {
        m_chunks->emplace_back(std::vector<uint8_t>{static_cast<uint8_t>(value)});
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    void chunked_memory_output_stream<R>::reserve(std::size_t size)
    {
        m_chunks->reserve(size);
    }

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    void chunked_memory_output_stream<R>::reserve(const std::function<std::size_t()>& calculate_reserve_size)
    {
        m_chunks->reserve(calculate_reserve_size());
    }

    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    size_t chunked_memory_output_stream<R>::size() const
    {
        return std::accumulate(
            m_chunks->begin(),
            m_chunks->end(),
            0,
            [](size_t acc, const auto& chunk)
            {
                return acc + chunk.size();
            }
        );
    }
}