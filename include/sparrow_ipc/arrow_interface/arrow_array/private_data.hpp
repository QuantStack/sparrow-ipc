#pragma once
#include <concepts>
#include <cstdint>
#include <vector>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    template <typename T>
    concept ArrowPrivateData = requires(T& t)
    {
        { t.buffers_ptrs() } -> std::same_as<const void**>;
        { t.n_buffers() } -> std::convertible_to<std::size_t>;
    };

    class owning_arrow_array_private_data
    {
    public:

        explicit owning_arrow_array_private_data(std::vector<std::vector<std::uint8_t>>&& buffers);

        [[nodiscard]] SPARROW_IPC_API const void** buffers_ptrs() noexcept;
        [[nodiscard]] SPARROW_IPC_API std::size_t n_buffers() const noexcept;

    private:
        std::vector<std::vector<std::uint8_t>> m_buffers;
        std::vector<const void*> m_buffer_pointers;
    };

    class non_owning_arrow_array_private_data
    {
    public:

        explicit constexpr non_owning_arrow_array_private_data(std::vector<std::uint8_t*>&& buffers_pointers)
            : m_buffer_pointers(std::move(buffers_pointers))
        {
        }

        [[nodiscard]] SPARROW_IPC_API const void** buffers_ptrs() noexcept;
        [[nodiscard]] SPARROW_IPC_API std::size_t n_buffers() const noexcept;

    private:
        std::vector<std::uint8_t*> m_buffer_pointers;
    };
}
