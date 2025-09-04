#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc::utils
{
    const sparrow::dynamic_bitset_view<const std::uint8_t> message_buffer_to_validity_bitmap(
        const org::apache::arrow::flatbuf::RecordBatch* record_batch,
        std::span<const uint8_t> body,
        size_t index
    )
    {
        const auto buffer_metadata = record_batch->buffers()->Get(index);
        return sparrow::dynamic_bitset_view<const std::uint8_t>{
            body.data() + buffer_metadata->offset(),
            static_cast<size_t>(buffer_metadata->length())
        };
    }

    std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t index
    )
    {
        const auto bitmap_metadata = record_batch.buffers()->Get(index);
        if (bitmap_metadata->length() == 0)
        {
            return {nullptr, 0};
        }

        auto ptr = const_cast<uint8_t*>(body.data() + bitmap_metadata->offset());
        const sparrow::dynamic_bitset_view<const std::uint8_t> bitmap_view{
            ptr,
            static_cast<size_t>(record_batch.length())
        };
        return {ptr, bitmap_view.null_count()};
    }
}