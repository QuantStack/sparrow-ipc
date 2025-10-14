#include "sparrow_ipc/arrow_interface/arrow_array/private_data.hpp"

namespace sparrow_ipc
{
    owning_arrow_array_private_data::owning_arrow_array_private_data(std::vector<std::vector<std::uint8_t>>&& buffers)
        : m_buffers(std::move(buffers))
    {
        m_buffer_pointers.reserve(m_buffers.size());
        for (const auto& buffer : m_buffers)
        {
            m_buffer_pointers.push_back(buffer.data());
        }
    }

    const void** owning_arrow_array_private_data::buffers_ptrs() noexcept
    {
        return m_buffer_pointers.data();
    }

    std::size_t owning_arrow_array_private_data::n_buffers() const noexcept
    {
        return m_buffers.size();
    }

    const void** non_owning_arrow_array_private_data::buffers_ptrs() noexcept
    {
        return const_cast<const void**>(reinterpret_cast<void**>(m_buffer_pointers.data()));
    }

    std::size_t non_owning_arrow_array_private_data::n_buffers() const noexcept
    {
        return m_buffer_pointers.size();
    }
}
