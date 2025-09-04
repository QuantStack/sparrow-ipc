#pragma once

#include <span>

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/variable_size_binary_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"
#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
{
    template <typename T>
    [[nodiscard]] T deserialize_variable_size_binary(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        size_t& buffer_index
    )
    {
        const std::string_view format = data_type_to_format(sparrow::detail::get_data_type_from_array<T>::get());
        ArrowSchema schema = make_non_owning_arrow_schema(
            format,
            name.data(),
            metadata,
            std::nullopt,
            0,
            nullptr,
            nullptr
        );
        const auto [bitmap_ptr, null_count] = utils::get_bitmap_pointer_and_null_count(
            record_batch,
            body,
            buffer_index++
        );
        const auto offset_metadata = record_batch.buffers()->Get(buffer_index++);
        auto offset_ptr = const_cast<uint8_t*>(body.data() + offset_metadata->offset());
        const auto buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto buffer_ptr = const_cast<uint8_t*>(body.data() + buffer_metadata->offset());
        std::vector<std::uint8_t*> buffers = {bitmap_ptr, offset_ptr, buffer_ptr};
        ArrowArray array = make_non_owning_arrow_array(
            record_batch.length(),
            null_count,
            0,
            std::move(buffers),
            0,
            nullptr,
            nullptr
        );
        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return T{std::move(ap)};
    }
}