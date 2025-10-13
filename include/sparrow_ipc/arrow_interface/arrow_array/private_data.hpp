#pragma once

#include <cstdint>
#include <vector>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    class owning_arrow_array_private_data
    {
    public:

        explicit owning_arrow_array_private_data(std::vector<std::vector<std::uint8_t>>&& buffers)
            : m_buffers(std::move(buffers))
        {
            m_buffer_pointers.reserve(m_buffers.size());
            for (const auto& buffer : m_buffers)
            {
                m_buffer_pointers.push_back(buffer.data());
            }
        }

        const void** buffers_ptrs() noexcept
        {
            return m_buffer_pointers.data();
        }

        std::size_t n_buffers() const noexcept
        {
            return m_buffers.size();
        }

    private:

        std::vector<std::vector<std::uint8_t>> m_buffers;
        std::vector<const void*> m_buffer_pointers;
    };

    class non_owning_arrow_array_private_data
    {
    public:

        explicit constexpr non_owning_arrow_array_private_data(std::vector<std::uint8_t*>&& buffers_pointers)
            : m_buffers_pointers(std::move(buffers_pointers))
        {
        }

        [[nodiscard]] SPARROW_IPC_API const void** buffers_ptrs() noexcept;

    private:

        std::vector<std::uint8_t*> m_buffers_pointers;
    };
}
