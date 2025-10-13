#pragma once

#include <cstdint>
#include <numeric>
#include <ranges>

#include "sparrow_ipc/output_stream.hpp"

namespace sparrow_ipc
{
    template <typename R>
        requires std::ranges::random_access_range<R>
                 && std::ranges::random_access_range<std::ranges::range_value_t<R>>
                 && std::same_as<typename std::ranges::range_value_t<R>::value_type, uint8_t>
    class chunked_memory_output_stream final : public output_stream
    {
    public:

        explicit chunked_memory_output_stream(R& chunks)
            : m_chunks(&chunks) {};

        chunked_memory_output_stream<R>& write(const char* s, std::streamsize count) final
        {
            m_chunks->emplace_back(s, s + count);
            return *this;
        }

        chunked_memory_output_stream<R>& write(std::span<const std::uint8_t> span) final
        {
            m_chunks->emplace_back(span.begin(), span.end());
            return *this;
        }

        chunked_memory_output_stream<R>& write(std::vector<uint8_t>&& buffer)
        {
            m_chunks->emplace_back(std::move(buffer));
            return *this;
        }

        chunked_memory_output_stream<R>& write(uint8_t value, std::size_t count) final
        {
            m_chunks->emplace_back(count, value);
            return *this;
        }

        chunked_memory_output_stream<R>& put(char value) final
        {
            m_chunks->emplace_back(std::vector<uint8_t>{static_cast<uint8_t>(value)});
            return *this;
        }

        void reserve(std::size_t size) override
        {
            m_chunks->reserve(size);
        }

        void reserve(const std::function<std::size_t()>& calculate_reserve_size) override
        {
            m_chunks->reserve(calculate_reserve_size());
        }

        [[nodiscard]] size_t size() const override
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

        R* m_chunks;
    };
}