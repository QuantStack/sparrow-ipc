#pragma once

#include <span>

#include <sparrow/buffer/dynamic_bitset/dynamic_bitset_view.hpp>
#include <sparrow/u8_buffer.hpp>
#include <utility>

#include "Message_generated.h"
#include "Schema_generated.h"

namespace sparrow_ipc::utils
{
    template <typename T>
    [[nodiscard]] sparrow::u8_buffer<T> message_buffer_to_u8buffer(
        const org::apache::arrow::flatbuf::RecordBatch* record_batch,
        std::span<const uint8_t> body,
        size_t index
    )
    {
        const auto buffer_metadata = record_batch->buffers()->Get(index);
        auto ptr = const_cast<uint8_t*>(body.data() + buffer_metadata->offset());
        auto casted_ptr = reinterpret_cast<T*>(ptr);
        const std::size_t count = static_cast<std::size_t>(buffer_metadata->length() / sizeof(T));
        return sparrow::u8_buffer<T>{casted_ptr, count};
    }

    [[nodiscard]] const sparrow::dynamic_bitset_view<const std::uint8_t> message_buffer_to_validity_bitmap(
        const org::apache::arrow::flatbuf::RecordBatch* record_batch,
        std::span<const uint8_t> body,
        size_t index
    );

    [[nodiscard]] std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t index
    );
}