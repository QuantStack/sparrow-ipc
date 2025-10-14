#pragma once

#include <utility>

#include <sparrow/c_interface.hpp>
#include <sparrow/utils/contracts.hpp>

#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/arrow_interface/arrow_array/private_data.hpp"

namespace sparrow_ipc
{
    SPARROW_IPC_API void release_arrow_array_children_and_dictionary(ArrowArray* array);

    template <ArrowPrivateData T>
    void arrow_array_release(ArrowArray* array)
    {
        SPARROW_ASSERT_TRUE(array != nullptr)
        SPARROW_ASSERT_TRUE(array->release == std::addressof(arrow_array_release<T>))

        SPARROW_ASSERT_TRUE(array->private_data != nullptr);

        delete static_cast<T*>(array->private_data);
        array->private_data = nullptr;
        array->buffers = nullptr;  // The buffers were deleted with the private data

        release_arrow_array_children_and_dictionary(array);
        array->release = nullptr;
    }

    template <ArrowPrivateData T, typename Arg>
    void fill_arrow_array(
        ArrowArray& array,
        int64_t length,
        int64_t null_count,
        int64_t offset,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary,
        Arg&& private_data_arg
    )
    {
        SPARROW_ASSERT_TRUE(length >= 0);
        SPARROW_ASSERT_TRUE(null_count >= -1);
        SPARROW_ASSERT_TRUE(offset >= 0);

        array.length = length;
        array.null_count = null_count;
        array.offset = offset;
        array.n_children = static_cast<int64_t>(children_count);
        array.children = children;
        array.dictionary = dictionary;

        auto private_data = new T(std::forward<Arg>(private_data_arg));
        array.private_data = private_data;
        array.n_buffers = private_data->n_buffers();
        array.buffers = private_data->buffers_ptrs();

        array.release = &arrow_array_release<T>;
    }

    template <ArrowPrivateData T, typename Arg>
    [[nodiscard]] ArrowArray make_arrow_array(
        int64_t length,
        int64_t null_count,
        int64_t offset,
        size_t children_count,
        ArrowArray** children,
        ArrowArray* dictionary,
        Arg&& private_data_arg
    )
    {
        ArrowArray array{};
        fill_arrow_array<T>(
            array,
            length,
            null_count,
            offset,
            children_count,
            children,
            dictionary,
            std::forward<Arg>(private_data_arg)
        );
        return array;
    }
}
