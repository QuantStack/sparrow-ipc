
#include <cstdint>
#include <ranges>

#include "sparrow_ipc/output_stream.hpp"

namespace sparrow_ipc
{
    /**
     * @brief An output stream that writes data to a contiguous memory buffer.
     *
     * This template class implements an output_stream that appends data to a contiguous
     * random-access range (typically std::vector<uint8_t>). All write operations append
     * data to the end of the buffer, making it grow as needed.
     *
     * @tparam R A random access range type with uint8_t as its value type.
     *           Typically std::vector<uint8_t> or similar contiguous container types.
     *
     * @details The memory output stream:
     * - Supports efficient append operations
     * - Can reserve capacity to minimize reallocations
     * - Always operates on a contiguous memory buffer
     * - Stores a non-owning reference to the buffer
     *
     * @note The caller must ensure the buffer remains valid for the lifetime of this stream
     */
    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    class memory_output_stream final : public output_stream
    {
    public:

        /**
         * @brief Constructs a memory output stream with a reference to a buffer.
         *
         * @param buffer Reference to the container that will store the written data.
         *               The stream stores a non-owning pointer to this buffer for write operations.
         *
         * @note The caller must ensure the buffer remains valid for the lifetime of this stream
         */
        memory_output_stream(R& buffer)
            : m_buffer(&buffer) {};

        /**
         * @brief Writes character data to the buffer.
         *
         * Appends the specified character data to the end of the buffer.
         *
         * @param s Pointer to the character data to write
         * @param count Number of characters to write
         * @return Reference to this stream for method chaining
         *
         * @note The characters are converted to uint8_t and appended to the buffer
         */
        memory_output_stream& write(const char* s, std::streamsize count) final;

        /**
         * @brief Writes a span of bytes to the buffer.
         *
         * Appends the data from the provided span to the end of the buffer.
         *
         * @param span A span of bytes to write
         * @return Reference to this stream for method chaining
         */
        memory_output_stream& write(std::span<const std::uint8_t> span) final;

        /**
         * @brief Writes a byte value repeated a specified number of times.
         *
         * Appends the specified byte value repeated count times to the end of the buffer.
         * This is useful for padding operations or filling with a specific value.
         *
         * @param value The byte value to write
         * @param count Number of times to repeat the value
         * @return Reference to this stream for method chaining
         */
        memory_output_stream& write(uint8_t value, std::size_t count) final;

        /**
         * @brief Writes a single character to the buffer.
         *
         * Appends a single byte to the end of the buffer. The character is cast to uint8_t.
         *
         * @param value The character value to write
         * @return Reference to this stream for method chaining
         */
        memory_output_stream& put(char value) final;

        /**
         * @brief Reserves capacity in the underlying buffer.
         *
         * Reserves space for at least the specified number of bytes in the buffer.
         * This can help minimize reallocations during subsequent write operations.
         *
         * @param size Number of bytes to reserve
         */
        void reserve(std::size_t size) override;

        /**
         * @brief Reserves capacity using a lazy calculation function.
         *
         * Calls the provided function to determine the buffer size to reserve.
         *
         * @param calculate_reserve_size Function that returns the number of bytes to reserve
         */
        void reserve(const std::function<std::size_t()>& calculate_reserve_size) override;

        /**
         * @brief Gets the current size of the buffer.
         *
         * @return The number of bytes currently in the buffer
         */
        [[nodiscard]] size_t size() const override;

    private:

        R* m_buffer;
    };

    // Implementation

    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    memory_output_stream<R>& memory_output_stream<R>::write(const char* s, std::streamsize count)
    {
        m_buffer->insert(m_buffer->end(), s, s + count);
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    memory_output_stream<R>& memory_output_stream<R>::write(std::span<const std::uint8_t> span)
    {
        m_buffer->insert(m_buffer->end(), span.begin(), span.end());
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    memory_output_stream<R>& memory_output_stream<R>::write(uint8_t value, std::size_t count)
    {
        m_buffer->insert(m_buffer->end(), count, value);
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    memory_output_stream<R>& memory_output_stream<R>::put(char value)
    {
        m_buffer->push_back(static_cast<uint8_t>(value));
        return *this;
    }

    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    void memory_output_stream<R>::reserve(std::size_t size)
    {
        m_buffer->reserve(size);
    }

    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    void memory_output_stream<R>::reserve(const std::function<std::size_t()>& calculate_reserve_size)
    {
        m_buffer->reserve(calculate_reserve_size());
    }

    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    size_t memory_output_stream<R>::size() const
    {
        return m_buffer->size();
    }
}
