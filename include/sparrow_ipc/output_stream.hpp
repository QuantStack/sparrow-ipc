#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <ios>
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
        virtual output_stream& write(const char* s, std::streamsize count) = 0;

        virtual output_stream& write(std::span<const std::uint8_t> span) = 0;

        virtual output_stream& write(uint8_t value, std::size_t count) = 0;

        virtual output_stream& put(char value) = 0;

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

        [[nodiscard]] virtual size_t size() const = 0;

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
        [[nodiscard]] virtual bool is_open() const = 0;
    };
}  // namespace sparrow_ipc
