#include "sparrow_ipc/deserialize_utils.hpp"

#include "sparrow_ipc/compression.hpp"

namespace sparrow_ipc::utils
{
    std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        std::span<const uint8_t> validity_buffer_span,
        const int64_t length
    )
    {
        if (validity_buffer_span.empty())
        {
            return {nullptr, 0};
        }
        auto ptr = const_cast<uint8_t*>(validity_buffer_span.data());
        const sparrow::dynamic_bitset_view<const std::uint8_t> bitmap_view{
            ptr,
            static_cast<size_t>(length)
        };
        return {ptr, bitmap_view.null_count()};
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
        if (body.size() < (buffer_metadata->offset() + buffer_metadata->length()))
        {
            throw std::runtime_error("Buffer metadata exceeds body size");
        }
        auto buffer_span = body.subspan(buffer_metadata->offset(), buffer_metadata->length());

        if (compression)
        {
            auto decompressed_result = decompress(compression->codec(), buffer_span);
            return std::visit(
                [&decompressed_storage](auto&& arg) -> std::span<const uint8_t>
                {
                    using T = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<T, std::vector<uint8_t>>)
                    {
                        // Decompression happened
                        decompressed_storage.emplace_back(std::move(arg));
                        return decompressed_storage.back();
                    }
                    else // It's a std::span<const uint8_t>
                    {
                        // No decompression, but we are in a compression context, so we must copy the buffer
                        // to ensure the owning ArrowArray has access to all its data.
                        // TODO think about other strategies
                        decompressed_storage.emplace_back(arg.begin(), arg.end());
                        return decompressed_storage.back();
                    }
                },
                decompressed_result
            );
        }
        return buffer_span;
    }
}
