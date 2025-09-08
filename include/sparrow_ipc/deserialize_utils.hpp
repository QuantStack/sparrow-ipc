#pragma once

#include <span>
#include <utility>

#include <sparrow/buffer/dynamic_bitset/dynamic_bitset_view.hpp>
#include <sparrow/u8_buffer.hpp>

#include "Message_generated.h"
#include "Schema_generated.h"

namespace sparrow_ipc::utils
{
    /**
     * @brief Extracts bitmap pointer and null count from a RecordBatch buffer.
     *
     * This function retrieves a bitmap buffer from the specified index in the RecordBatch's
     * buffer list and calculates the number of null values represented by the bitmap.
     *
     * @param record_batch The Arrow RecordBatch containing buffer metadata
     * @param body The raw buffer data as a byte span
     * @param index The index of the bitmap buffer in the RecordBatch's buffer list
     *
     * @return A pair containing:
     *         - First: Pointer to the bitmap data (nullptr if buffer is empty)
     *         - Second: Count of null values in the bitmap (0 if buffer is empty)
     *
     * @note If the bitmap buffer has zero length, returns {nullptr, 0}
     * @note The returned pointer is a non-const cast of the original const data
     */
    [[nodiscard]] std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t index
    );
}