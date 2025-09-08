#include "sparrow_ipc/arrow_interface/arrow_array.hpp"

#include <cstdint>

#include <sparrow/c_interface.hpp>
#include <sparrow/utils/contracts.hpp>

#include "sparrow_ipc/arrow_interface/arrow_array/private_data.hpp"
#include "sparrow_ipc/arrow_interface/arrow_array_schema_common_release.hpp"

namespace sparrow_ipc
{
    void release_non_owning_arrow_array(ArrowArray* array)
    {
        SPARROW_ASSERT_FALSE(array == nullptr)
        SPARROW_ASSERT_TRUE(array->release == std::addressof(release_non_owning_arrow_array))

        release_common_non_owning_arrow(*array);
        array->buffers = nullptr;  // The buffers were deleted with the private data
    }

    void fill_non_owning_arrow_array(
        ArrowArray& array,
        int64_t length,
        int64_t null_count,
        int64_t offset,
        std::vector<std::uint8_t*>&& buffers,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary
    )
    {
        SPARROW_ASSERT_TRUE(length >= 0);
        SPARROW_ASSERT_TRUE(null_count >= -1);
        SPARROW_ASSERT_TRUE(offset >= 0);

        array.length = length;
        array.null_count = null_count;
        array.offset = offset;
        array.n_buffers = static_cast<int64_t>(buffers.size());
        array.private_data = new non_owning_arrow_array_private_data(std::move(buffers));
        const auto private_data = static_cast<non_owning_arrow_array_private_data*>(array.private_data);
        array.buffers = private_data->buffers_ptrs();
        array.n_children = static_cast<int64_t>(children_count);
        array.children = children;
        array.dictionary = dictionary;
        array.release = release_non_owning_arrow_array;
    }

    ArrowArray make_non_owning_arrow_array(
        int64_t length,
        int64_t null_count,
        int64_t offset,
        std::vector<std::uint8_t*>&& buffers,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary
    )
    {
        ArrowArray array{};
        fill_non_owning_arrow_array(
            array,
            length,
            null_count,
            offset,
            std::move(buffers),
            children_count,
            children,
            dictionary
        );
        return array;
    }
}
