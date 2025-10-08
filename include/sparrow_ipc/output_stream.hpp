#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Abstract interface for output streams used in sparrow-ipc serialization.
     *
     * This interface provides a generic way to write binary data during serialization
     * operations. Implementations can target different destinations such as files,
     * memory buffers, network streams, etc.
     */
    class SPARROW_IPC_API output_stream
    {
    public:

        virtual ~output_stream() = default;

        /**
         * @brief Writes a span of bytes to the output stream.
         *
         * This method attempts to write all bytes from the provided span to the
         * underlying destination. It returns the number of bytes actually written,
         * which may be less than the requested size in case of errors or partial writes.
         *
         * @param span A span of bytes to write
         * @return Number of bytes successfully written
         * @throws std::runtime_error if a write error occurs
         */
        virtual std::size_t write(std::span<const std::uint8_t> span) = 0;

        virtual std::size_t write(uint8_t value, std::size_t count = 1) = 0;

        void add_padding()
        {
            const size_t current_size = size();
            const size_t padding_needed = (8 - (current_size % 8)) % 8;
            if (padding_needed > 0)
            {
                write(uint8_t{0}, padding_needed);
            }
        }

        /**
         * @brief Reserves capacity in the output stream if supported.
         *
         * This is a hint to the implementation that at least `size` bytes
         * will be written. Implementations may use this to optimize memory
         * allocation or buffer management.
         *
         * @param size Number of bytes to reserve
         * @return true if reservation was successful or not needed, false otherwise
         */
        virtual void reserve(std::size_t size) = 0;

        virtual void reserve(const std::function<std::size_t()>& calculate_reserve_size) = 0;

        virtual size_t size() const = 0;

        /**
         * @brief Flushes any buffered data to the underlying destination.
         *
         * Ensures that all previously written data is committed to the
         * underlying storage or transmitted.
         *
         * @throws std::runtime_error if flush operation fails
         */
        virtual void flush() = 0;

        /**
         * @brief Closes the output stream and releases any resources.
         *
         * After calling close(), no further operations should be performed
         * on the stream. After calling close(), is_open() should return false.
         * Multiple calls to close() should be safe.
         *
         * @throws std::runtime_error if close operation fails
         */
        virtual void close() = 0;

        /**
         * @brief Checks if the stream is still open and writable.
         *
         * @return true if the stream is open, false if closed or in error state
         */
        virtual bool is_open() const = 0;

        // Convenience method for writing single bytes
        std::size_t write(std::uint8_t byte)
        {
            return write(std::span<const std::uint8_t, 1>{&byte, 1});
        }
    };
}  // namespace sparrow_ipc
