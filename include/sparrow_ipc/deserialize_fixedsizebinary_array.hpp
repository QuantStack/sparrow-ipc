#pragma once

#include <sparrow/arrow_interface/arrow_array_schema_proxy.hpp>
#include <sparrow/fixed_width_binary_array.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/arrow_interface/arrow_array.hpp"
#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"


namespace sparrow_ipc
{

    [[nodiscard]] sparrow::fixed_width_binary_array deserialize_fixedwidthbinary(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        size_t& buffer_index,
        int32_t byte_width
    )
    {
        const std::string format = "w:" + std::to_string(byte_width);
        ArrowSchema schema = make_arrow_schema(format, name.data(), metadata, std::nullopt, 0, nullptr, nullptr);

        const auto buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto buffer_ptr = const_cast<uint8_t*>(body.data() + buffer_metadata->offset());
        const size_t buffer_size = buffer_metadata->length();

        const auto bitmap_buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto bitmap_ptr = const_cast<uint8_t*>(body.data() + bitmap_buffer_metadata->offset());

        const sparrow::dynamic_bitset_view<const std::uint8_t> bitmap_view{bitmap_ptr, static_cast<size_t>(record_batch.length())};
        std::vector<std::uint8_t*> buffers = {buffer_ptr, bitmap_ptr};

        ArrowArray array = make_arrow_array(record_batch.length(), bitmap_view.null_count(), 0, std::move(buffers), 0, nullptr, nullptr);

        sparrow::arrow_proxy ap{std::move(array), std::move(schema)};
        return sparrow::fixed_width_binary_array{std::move(ap)};
    }
}