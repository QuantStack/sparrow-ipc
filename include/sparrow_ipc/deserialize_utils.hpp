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
     * @brief Extracts bitmap pointer and null count from a validity buffer span.
     *
     * This function calculates the number of null values represented by the bitmap.
     *
     * @param validity_buffer_span The validity buffer as a byte span.
     * @param length The Arrow RecordBatch length (number of values in the array).
     *
     * @return A pair containing:
     *         - First: Pointer to the bitmap data (nullptr if buffer is empty)
     *         - Second: Count of null values in the bitmap (0 if buffer is empty)
     *
     * @note If the bitmap buffer is empty, returns {nullptr, 0}
     * @note The returned pointer is a non-const cast of the original const data
     */
    [[nodiscard]] std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        std::span<const uint8_t> validity_buffer_span,
        const int64_t length
    );

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
    // TODO to be removed when not used anymore (after adding compression to deserialize_fixedsizebinary_array)
    [[nodiscard]] std::pair<std::uint8_t*, int64_t> get_bitmap_pointer_and_null_count(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t index
    );

    /**
     * @brief Extracts a buffer from a RecordBatch and decompresses it if necessary.
     *
     * This function retrieves a buffer span from the specified index, increments the index,
     * and applies decompression if specified.
     *
     * @param record_batch The Arrow RecordBatch containing buffer metadata.
     * @param body The raw buffer data as a byte span.
     * @param buffer_index The index of the buffer to retrieve. This value is incremented by the function.
     * @param compression The compression algorithm to use. If nullptr, no decompression is performed.
     *
     * @return A `std::variant` containing either:
     *         - A `std::vector<std::uint8_t>` if the buffer was decompressed, owning the newly allocated data.
     *         - A `std::span<const std::uint8_t>` if no decompression occurred, providing a view of the original `body`.
     */
    [[nodiscard]] std::variant<std::vector<std::uint8_t>, std::span<const std::uint8_t>> get_decompressed_buffer(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t& buffer_index,
        const org::apache::arrow::flatbuf::BodyCompression* compression
    );

    /**
     * @brief Extracts a buffer from a RecordBatch's body.
     *
     * This function retrieves a buffer span from the specified index in the RecordBatch's
     * buffer list and increments the index.
     *
     * @param record_batch The Arrow RecordBatch containing buffer metadata.
     * @param body The raw buffer data as a byte span.
     * @param buffer_index The index of the buffer to retrieve. This value is incremented by the function.
     *
     * @return A `std::span<const uint8_t>` viewing the extracted buffer data.
     * @throws std::runtime_error if the buffer metadata indicates a buffer that exceeds the body size.
     */
    [[nodiscard]] std::span<const uint8_t> get_buffer(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        std::span<const uint8_t> body,
        size_t& buffer_index
    );
}
