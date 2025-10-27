#include "sparrow_ipc/arrow_interface/arrow_array/private_data.hpp"

namespace sparrow_ipc
{
    arrow_array_private_data::arrow_array_private_data(std::vector<optionally_owned_buffer>&& buffers)
        : m_buffers(std::move(buffers))
    {
        m_buffer_pointers.reserve(m_buffers.size());
        for (const auto& buffer : m_buffers)
        {
            std::visit(
                [this](auto&& arg)
                {
                    if (arg.empty())
                    {
                        m_buffer_pointers.push_back(nullptr);
                    }
                    else
                    {
                        m_buffer_pointers.push_back(arg.data());
                    }
                },
                buffer
            );
        }
    }

    const void** arrow_array_private_data::buffers_ptrs() noexcept
    {
        return m_buffer_pointers.data();
    }

    std::size_t arrow_array_private_data::n_buffers() const noexcept
    {
        return m_buffers.size();
    }
}
