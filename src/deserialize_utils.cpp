#include "sparrow_ipc/deserialize_utils.hpp"

#include "sparrow_ipc/compression.hpp"

namespace sparrow_ipc::utils
{
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

    std::span<const uint8_t> get_and_decompress_buffer(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t& buffer_index,
        const org::apache::arrow::flatbuf::BodyCompression* compression,
        std::vector<std::vector<uint8_t>>& decompressed_storage
    )
    {
        const auto buffer_metadata = record_batch.buffers()->Get(buffer_index++);
        auto buffer_span = body.subspan(buffer_metadata->offset(), buffer_metadata->length());

        if (compression)
        {
            decompressed_storage.emplace_back(decompress(compression->codec(), buffer_span));
            buffer_span = decompressed_storage.back();
        }
        return buffer_span;
    }
}