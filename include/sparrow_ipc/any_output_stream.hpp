#pragma once

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Concept for stream-like types that support write operations.
     *
     * A type satisfies this concept if it has a write method that accepts
     * a span of bytes and returns the number of bytes written.
     */
    template <typename T>
    concept writable_stream = requires(T& t, const char* s, std::streamsize count) {
        { t.write(s, count) };
    };

    /**
     * @brief Type-erased wrapper for any stream-like object.
     *
     * This class provides type erasure for ANY type that supports stream operations,
     * not just those derived from output_stream. It uses the concept-based type erasure
     * pattern to wrap any stream-like object polymorphically.
     *
     * @details This implementation uses the classic type erasure pattern with:
     * - An abstract base class (stream_concept) defining the interface
     * - A templated model class that adapts any stream type to the interface
     * - A wrapper class that stores the model polymorphically
     *
     * Usage:
     * @code
     * // Works with output_stream derived classes
     * std::vector<uint8_t> buffer;
     * memory_output_stream<std::vector<uint8_t>> mem_stream(buffer);
     * any_output_stream stream1(mem_stream);
     *
     * // Also works with standard streams
     * std::ostringstream oss;
     * any_output_stream stream2(oss);
     *
     * // Or any custom type with a write method
     * my_custom_stream custom;
     * any_output_stream stream3(custom);
     * @endcode
     *
     * The class provides a common interface that works with any stream type.
     */
    class SPARROW_IPC_API any_output_stream
    {
    public:

        /**
         * @brief Constructs a type-erased stream from any stream-like object.
         *
         * @tparam TStream The concrete stream type (must satisfy writable_stream concept)
         * @param stream The stream object to wrap
         *
         * The stream is stored by reference, so the caller must ensure the stream
         * lifetime exceeds that of the any_output_stream object.
         */
        template <writable_stream TStream>
        any_output_stream(TStream& stream)
            : m_impl(std::make_unique<stream_model<TStream>>(stream))
        {
        }

        /**
         * @brief Default destructor.
         */
        ~any_output_stream() = default;

        any_output_stream(any_output_stream&&) noexcept = default;
        any_output_stream& operator=(any_output_stream&&) noexcept = default;

        any_output_stream(const any_output_stream&) = delete;
        any_output_stream& operator=(const any_output_stream&) = delete;

        /**
         * @brief Writes a span of bytes to the underlying stream.
         *
         * @param span The bytes to write
         * @return The number of bytes written
         * @throws std::runtime_error if write operation fails
         */
        void write(std::span<const std::uint8_t> span)
        {
            m_impl->write(span);
        }

        /**
         * @brief Writes a single byte value multiple times.
         *
         * @param value The byte value to write
         * @param count Number of times to write the value (default: 1)
         * @return The number of bytes written
         */
        void write(uint8_t value, std::size_t count = 1)
        {
            m_impl->write(value, count);
        }

        /**
         * @brief Adds padding to align to 8-byte boundary.
         */
        void add_padding()
        {
            m_impl->add_padding();
        }

        /**
         * @brief Reserves capacity if supported by the underlying stream.
         *
         * @param size The number of bytes to reserve
         */
        void reserve(std::size_t size)
        {
            m_impl->reserve(size);
        }

        /**
         * @brief Reserves capacity using a lazy calculation function.
         *
         * @param calculate_reserve_size Function that calculates the size to reserve
         */
        void reserve(const std::function<std::size_t()>& calculate_reserve_size)
        {
            m_impl->reserve(calculate_reserve_size);
        }

        /**
         * @brief Gets the current size of the stream.
         *
         * @return The current number of bytes written
         */
        [[nodiscard]] size_t size() const
        {
            return m_impl->size();
        }

        /**
         * @brief Flushes buffered data to the underlying destination.
         */
        void flush()
        {
            m_impl->flush();
        }

        /**
         * @brief Closes the stream and releases resources.
         */
        void close()
        {
            m_impl->close();
        }

        /**
         * @brief Checks if the stream is open.
         *
         * @return true if open, false otherwise
         */
        bool is_open() const
        {
            return m_impl->is_open();
        }

        /**
         * @brief Gets a reference to the underlying stream cast to the specified type.
         *
         * @tparam TStream The expected concrete type of the underlying stream
         * @return Reference to the underlying stream as TStream
         * @throws std::bad_cast if the underlying stream is not of type TStream
         */
        template <typename TStream>
        TStream& get()
        {
            auto* model = dynamic_cast<stream_model<TStream>*>(m_impl.get());
            if (!model)
            {
                throw std::bad_cast();
            }
            return model->get_stream();
        }

        /**
         * @brief Gets a const reference to the underlying stream cast to the specified type.
         *
         * @tparam TStream The expected concrete type of the underlying stream
         * @return Const reference to the underlying stream as TStream
         * @throws std::bad_cast if the underlying stream is not of type TStream
         */
        template <typename TStream>
        const TStream& get() const
        {
            const auto* model = dynamic_cast<const stream_model<TStream>*>(m_impl.get());
            if (!model)
            {
                throw std::bad_cast();
            }
            return model->get_stream();
        }

    private:

        /**
         * @brief Abstract interface for type-erased streams.
         */
        struct stream_concept
        {
            virtual ~stream_concept() = default;
            virtual void write(const char* s, std::streamsize count) = 0;
            virtual void write(std::span<const std::uint8_t> span) = 0;
            virtual void write(uint8_t value, std::size_t count) = 0;
            virtual void put(uint8_t value) = 0;
            virtual void add_padding() = 0;
            virtual void reserve(std::size_t size) = 0;
            virtual void reserve(const std::function<std::size_t()>& calculate_reserve_size) = 0;
            [[nodiscard]] virtual size_t size() const = 0;
            virtual void flush() = 0;
            virtual void close() = 0;
            [[nodiscard]] virtual bool is_open() const = 0;
        };

        /**
         * @brief Concrete model that adapts a specific stream type to the interface.
         *
         * @tparam TStream The concrete stream type
         */
        template <typename TStream>
        class stream_model : public stream_concept
        {
        public:

            stream_model(TStream& stream)
                : m_stream(&stream)
            {
            }

            void write(const char* s, std::streamsize count) final
            {
                m_stream->write(s, count);
                m_size += static_cast<size_t>(count);
            }

            void write(std::span<const std::uint8_t> span) final
            {
                m_stream->write(reinterpret_cast<const char*>(span.data()), static_cast<std::streamsize>(span.size()));
                m_size += span.size();
            }

            void write(uint8_t value, std::size_t count) final
            {
                if constexpr (requires(TStream& t, uint8_t v, std::size_t c) { t.write(v, c); })
                {
                    m_stream->write(value, count);
                }
                else
                {
                    // Fallback: write one byte at a time
                    for (std::size_t i = 0; i < count; ++i)
                    {
                        m_stream->put(value);
                    }
                }
                m_size += count;
            }

            void put(uint8_t value) final
            {
                m_stream->put(value);
                m_size ++;
            }

            void add_padding() final
            {
                const size_t current_size = size();
                const size_t padding_needed = (8 - (current_size % 8)) % 8;
                if (padding_needed > 0)
                {
                    static constexpr char padding_value = 0;
                    for (size_t i = 0; i < padding_needed; ++i)
                    {
                        m_stream->write(&padding_value, 1);
                    }
                    m_size += padding_needed;
                }
            }

            void reserve(std::size_t size) final
            {
                if constexpr (requires(TStream& t, std::size_t s) { t.reserve(s); })
                {
                    m_stream->reserve(size);
                }
                // If not reservable, do nothing
            }

            void reserve(const std::function<std::size_t()>& calculate_reserve_size) final
            {
                if constexpr (requires(TStream& t, const std::function<std::size_t()>& func) {
                                  { t.reserve(func) };
                              })
                {
                    m_stream->reserve(calculate_reserve_size);
                }
                else if constexpr (requires(TStream& t, std::size_t s) { t.reserve(s); })
                {
                    m_stream->reserve(calculate_reserve_size());
                }
                // If not reservable, do nothing
            }

            [[nodiscard]] size_t size() const final
            {
                if constexpr (requires(const TStream& t) {
                                  { t.size() } -> std::convertible_to<size_t>;
                              })
                {
                    return m_stream->size();
                }
                else
                {
                    return m_size;
                }
            }

            void flush() final
            {
                if constexpr (requires(TStream& t) { t.flush(); })
                {
                    m_stream->flush();
                }
                // If no flush method, do nothing
            }

            void close() final
            {
                if constexpr (requires(TStream& t) { t.close(); })
                {
                    m_stream->close();
                }
                // If no close method, do nothing
            }

            [[nodiscard]] bool is_open() const final
            {
                if constexpr (requires(const TStream& t) {
                                  { t.is_open() } -> std::convertible_to<bool>;
                              })
                {
                    return m_stream->is_open();
                }
                else
                {
                    return true;  // Assume open if no method available
                }
            }

            TStream& get_stream()
            {
                return *m_stream;
            }

            const TStream& get_stream() const
            {
                return *m_stream;
            }

        private:

            TStream* m_stream;
            size_t m_size = 0;
        };

        std::unique_ptr<stream_concept> m_impl;
    };
}  // namespace sparrow_ipc
