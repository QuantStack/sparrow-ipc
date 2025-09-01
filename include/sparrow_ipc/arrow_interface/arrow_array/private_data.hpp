#pragma once

#include <vector>

namespace sparrow_ipc
{
    class arrow_array_private_data
    {
    public:

        explicit constexpr arrow_array_private_data(std::vector<std::uint8_t*>&& buffers_pointers)
            : m_buffers_pointers(std::move(buffers_pointers))
        {
        }

        [[nodiscard]] const void** buffers_ptrs() noexcept;

    private:

        std::vector<std::uint8_t*> m_buffers_pointers;
    };
}
