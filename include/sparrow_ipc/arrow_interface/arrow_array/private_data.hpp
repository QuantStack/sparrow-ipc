#pragma once

#include <cstdint>
#include <vector>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
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
