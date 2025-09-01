#include "sparrow_ipc/arrow_interface/arrow_array/private_data.hpp"

namespace sparrow_ipc
{
    const void** arrow_array_private_data::buffers_ptrs() noexcept
    {
        return const_cast<const void**>(reinterpret_cast<void**>(m_buffers_pointers.data()));
    }
}