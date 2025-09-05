#include "sparrow_ipc/deserialize_fixedsizebinary_array.hpp"

namespace sparrow_ipc
{
    sparrow::fixed_width_binary_array deserialize_non_owning_fixedwidthbinary(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        std::string_view name,
        const std::optional<std::vector<sparrow::metadata_pair>>& metadata,
        size_t& buffer_index,
        int32_t byte_width
    )
    {
        const std::string format = "w:" + std::to_string(byte_width);
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
        const auto buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto buffer_ptr = const_cast<uint8_t*>(body.data() + buffer_metadata->offset());
        std::vector<std::uint8_t*> buffers = {bitmap_ptr, buffer_ptr};
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
        return sparrow::fixed_width_binary_array{std::move(ap)};
    }
}
