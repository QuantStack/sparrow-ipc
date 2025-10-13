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
         * @brief Writes character data to the output stream.
         *
         * This method writes the specified number of characters from a C-style string
         * to the underlying destination.
         *
         * @param s Pointer to the character data to write
         * @param count Number of characters to write
         * @return Reference to this stream for method chaining
         * @throws std::runtime_error if a write error occurs
         */
        virtual output_stream& write(const char* s, std::streamsize count) = 0;

        /**
         * @brief Writes a span of bytes to the output stream.
         *
         * This method attempts to write all bytes from the provided span to the
         * underlying destination.
         *
         * @param span A span of bytes to write
         * @return Reference to this stream for method chaining
         * @throws std::runtime_error if a write error occurs
         */
        virtual output_stream& write(std::span<const std::uint8_t> span) = 0;

        /**
         * @brief Writes a byte value repeated a specified number of times.
         *
         * This method writes the same byte value multiple times to the stream,
         * useful for padding or filling operations.
         *
         * @param value The byte value to write
         * @param count Number of times to write the value
         * @return Reference to this stream for method chaining
         * @throws std::runtime_error if a write error occurs
         */
        virtual output_stream& write(uint8_t value, std::size_t count) = 0;

        /**
         * @brief Writes a single character to the output stream.
         *
         * @param value The character value to write
         * @return Reference to this stream for method chaining
         * @throws std::runtime_error if a write error occurs
         */
        virtual output_stream& put(char value) = 0;

        /**
         * @brief Reserves capacity in the output stream if supported.
         *
         * This is a hint to the implementation that at least `size` bytes
         * will be written. Implementations may use this to optimize memory
         * allocation or buffer management.
         *
         * @param size Number of bytes to reserve
         * @note Implementations that don't support reservation can provide a no-op implementation
         */
        virtual void reserve(std::size_t size) = 0;

        /**
         * @brief Reserves capacity using a lazy calculation function.
         *
         * This method allows deferred calculation of the required capacity, which can
         * be useful when the exact size depends on expensive computations that should
         * only be performed if the stream supports reservation.
         *
         * @param calculate_reserve_size Function that calculates the number of bytes to reserve
         * @note Implementations that don't support reservation can provide a no-op implementation
         */
        virtual void reserve(const std::function<std::size_t()>& calculate_reserve_size) = 0;

        /**
         * @brief Gets the current size of the stream.
         *
         * @return The current number of bytes written to the stream
         */
        [[nodiscard]] virtual size_t size() const = 0;
    };
}  // namespace sparrow_ipc
