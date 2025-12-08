#include "sparrow_ipc/any_output_stream.hpp"

namespace sparrow_ipc
{
    void any_output_stream::write(std::span<const std::uint8_t> span)
    {
        m_impl->write(span);
    }

    void any_output_stream::write(uint8_t value, std::size_t count)
    {
        m_impl->write(value, count);
    }

    void any_output_stream::add_padding()
    {
        m_impl->add_padding();
    }

    void any_output_stream::reserve(std::size_t size)
    {
        m_impl->reserve(size);
    }

    void any_output_stream::reserve(const std::function<std::size_t()>& calculate_reserve_size)
    {
        m_impl->reserve(calculate_reserve_size);
    }

    size_t any_output_stream::size() const
    {
        return m_impl->size();
    }

}  // namespace sparrow_ipc
