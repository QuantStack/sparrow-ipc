
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

        memory_output_stream& write(const char* s, std::streamsize count) final
        {
            m_buffer->insert(m_buffer->end(), s, s + count);
            return *this;
        }

        memory_output_stream& write(std::span<const std::uint8_t> span) final
        {
            m_buffer->insert(m_buffer->end(), span.begin(), span.end());
            return *this;
        }

        memory_output_stream& write(uint8_t value, std::size_t count) final
        {
            m_buffer->insert(m_buffer->end(), count, value);
            return *this;
        }

        memory_output_stream& put(char value) final
        {
            m_buffer->push_back(static_cast<uint8_t>(value));
            return *this;
        }

        void reserve(std::size_t size) override
        {
            m_buffer->reserve(size);
        }

        void reserve(const std::function<std::size_t()>& calculate_reserve_size) override
        {
            m_buffer->reserve(calculate_reserve_size());
        }

        [[nodiscard]] size_t size() const override
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

        [[nodiscard]] bool is_open() const override
        {
            return true;
        }

    private:

        R* m_buffer;
    };
}
