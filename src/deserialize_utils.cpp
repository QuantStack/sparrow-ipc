#include "sparrow_ipc/deserialize_utils.hpp"

namespace sparrow_ipc::utils
{
    std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t index
    )
    {
        if(index >= static_cast<size_t>(record_batch.buffers()->size()))
        {
            throw std::runtime_error("Buffer index out of range");
        }
        const auto bitmap_metadata = record_batch.buffers()->Get(index);
        if (bitmap_metadata->length() == 0)
        {
            return {nullptr, 0};
        }
        if (body.size() < (bitmap_metadata->offset() + bitmap_metadata->length()))
        {
            throw std::runtime_error("Bitmap buffer exceeds body size");
        }
        auto ptr = const_cast<uint8_t*>(body.data() + bitmap_metadata->offset());
        const sparrow::dynamic_bitset_view<const std::uint8_t> bitmap_view{
            ptr,
            static_cast<size_t>(record_batch.length())
        };
        return {ptr, bitmap_view.null_count()};
    }
}