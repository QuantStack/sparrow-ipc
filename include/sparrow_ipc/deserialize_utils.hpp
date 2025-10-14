#pragma once

#include <span>
#include <utility>
#include <vector>

#include <sparrow/buffer/dynamic_bitset/dynamic_bitset_view.hpp>
#include <sparrow/u8_buffer.hpp>

#include "Message_generated.h"

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

    /**
     * @brief Extracts a buffer from a RecordBatch and decompresses it if necessary.
     *
     * This function retrieves a buffer span from the specified index, increments the index,
     * and applies decompression if specified. If the buffer is decompressed, the new
     * data is stored in `decompressed_storage` and the returned span will point to this new data.
     *
     * @param record_batch The Arrow RecordBatch containing buffer metadata.
     * @param body The raw buffer data as a byte span.
     * @param buffer_index The index of the buffer to retrieve. This value is incremented by the function.
     * @param compression The compression algorithm to use. If nullptr, no decompression is performed.
     * @param decompressed_storage A vector that will be used to store the data of any decompressed buffers.
     *
     * @return A span viewing the resulting buffer data. This will be a view of the original
     *         `body` if no decompression occurs, or a view of the newly added buffer in
     *         `decompressed_storage` if decompression occurs.
     */
    [[nodiscard]] std::span<const uint8_t> get_and_decompress_buffer(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t& buffer_index,
        const org::apache::arrow::flatbuf::BodyCompression* compression,
        std::vector<std::vector<uint8_t>>& decompressed_storage
    );
}
