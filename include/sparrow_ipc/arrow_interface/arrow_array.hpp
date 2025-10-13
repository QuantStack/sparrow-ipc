#pragma once

#include <vector>

#include <sparrow/c_interface.hpp>

#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    [[nodiscard]] SPARROW_IPC_API ArrowArray make_owning_arrow_array(
        int64_t length,
        int64_t null_count,
        int64_t offset,
        std::vector<std::vector<std::uint8_t>>&& buffers,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary
    );

    [[nodiscard]] SPARROW_IPC_API ArrowArray make_non_owning_arrow_array(
        int64_t length,
        int64_t null_count,
        int64_t offset,
        std::vector<std::uint8_t*>&& buffers,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary
    );

    SPARROW_IPC_API void release_non_owning_arrow_array(ArrowArray* array);

    SPARROW_IPC_API void fill_non_owning_arrow_array(
        ArrowArray& array,
        int64_t length,
        int64_t null_count,
        int64_t offset,
        std::vector<std::uint8_t*>&& buffers,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary
    );
}