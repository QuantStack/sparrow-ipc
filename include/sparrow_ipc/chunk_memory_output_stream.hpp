#pragma once

#include <cstdint>
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

        std::size_t write(std::span<const std::uint8_t> span) override
        {
            m_chunks->emplace_back(span.begin(), span.end());
            return span.size();
        }

        std::size_t write(std::vector<uint8_t>&& buffer)
        {
            m_chunks->emplace_back(std::move(buffer));
            return m_chunks->back().size();
        }

        std::size_t write(uint8_t value, std::size_t count) override
        {
            m_chunks->emplace_back(count, value);
            return count;
        }

        void reserve(std::size_t size) override
        {
            m_chunks->reserve(size);
        }

        void reserve(const std::function<std::size_t()>& calculate_reserve_size) override
        {
            m_chunks->reserve(calculate_reserve_size());
        }

        size_t size() const override
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