#include <concepts>
#include <cstdint>
#include <ranges>

#include "sparrow_ipc/output_stream.hpp"

namespace sparrow_ipc
{
    template <typename R>
        requires std::ranges::random_access_range<R> && std::same_as<typename R::value_type, uint8_t>
    class memory_output_stream final : public output_stream
    {
    public:

        memory_output_stream(R& buffer)
            : m_buffer(&buffer) {};

        std::size_t write(std::span<const std::uint8_t> span) override
        {
            m_buffer->insert(m_buffer->end(), span.begin(), span.end());
            return span.size();
        }

        std::size_t write(uint8_t value, std::size_t count) override
        {
            m_buffer->insert(m_buffer->end(), count, value);
            return count;
        }

        void reserve(std::size_t size) override
        {
            m_buffer->reserve(size);
        }

        void reserve(const std::function<std::size_t()>& calculate_reserve_size) override
        {
            m_buffer->reserve(calculate_reserve_size());
        }

        size_t size() const override
        {
            return m_buffer->size();
        }

        void flush() override
        {
            // Implementation for flushing memory
        }

        void close() override
        {
            // Implementation for closing the stream
        }

        bool is_open() const override
        {
            return true;
        }

    private:

        R* m_buffer;
    };
}
