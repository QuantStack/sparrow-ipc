#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc
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
}