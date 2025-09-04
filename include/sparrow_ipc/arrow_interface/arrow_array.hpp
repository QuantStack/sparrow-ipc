
#pragma once

#include <vector>

#include <sparrow/c_interface.hpp>

namespace sparrow_ipc
{
    [[nodiscard]] ArrowArray make_non_owning_arrow_array(
        int64_t length,
        int64_t null_count,
        int64_t offset,
        std::vector<std::uint8_t*>&& buffers,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary
    );

    void release_non_owning_arrow_array(ArrowArray* array);

    void fill_non_owning_arrow_array(
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