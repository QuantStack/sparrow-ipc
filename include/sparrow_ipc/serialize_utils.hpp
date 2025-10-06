#pragma once

#include <ranges>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Serializes a record batch schema into a binary message format.
     *
     * This function creates a serialized schema message by combining continuation bytes,
     * a length prefix, the flatbuffer schema data, and padding to ensure 8-byte alignment.
     * The resulting format follows the Arrow IPC specification for schema messages.
     *
     * @param record_batch The record batch containing the schema to be serialized
     * @param stream The output stream where the serialized schema message will be written
     */
    SPARROW_IPC_API void
    serialize_schema_message(const sparrow::record_batch& record_batch, any_output_stream& stream);

    /**
     * @brief Serializes a record batch into a binary format following the Arrow IPC specification.
     *
     * This function converts a sparrow record batch into a serialized byte vector that includes:
     * - A continuation marker at the beginning
     * - The record batch metadata length (4 bytes)
     * - The FlatBuffer-encoded record batch metadata containing field nodes and buffer information
     * - Padding to ensure 8-byte alignment
     * - The actual data body containing the record batch buffers
     *
     * The serialization follows the Arrow IPC stream format where each record batch message
     * consists of a metadata section followed by a body section containing the actual data.
     *
     * @param record_batch The sparrow record batch to be serialized
     * @param compression The compression type to use when serializing
     * @param stream The output stream where the serialized record batch will be written
     */
    SPARROW_IPC_API void
    serialize_record_batch(const sparrow::record_batch& record_batch, any_output_stream& stream, std::optional<org::apache::arrow::flatbuf::CompressionType> compression);

    /**
     * @brief Calculates the total serialized size of a schema message.
     *
     * This function computes the complete size that would be produced by serialize_schema_message(),
     * including:
     * - Continuation bytes (4 bytes)
     * - Message length prefix (4 bytes)
     * - FlatBuffer schema message data
     * - Padding to 8-byte alignment
     *
     * @param record_batch The record batch containing the schema to be measured
     * @return The total size in bytes that the serialized schema message would occupy
     */
    [[nodiscard]] SPARROW_IPC_API std::size_t
    calculate_schema_message_size(const sparrow::record_batch& record_batch);

    /**
     * @brief Calculates the total serialized size of a record batch message.
     *
     * This function computes the complete size that would be produced by serialize_record_batch(),
     * including:
     * - Continuation bytes (4 bytes)
     * - Message length prefix (4 bytes)
     * - FlatBuffer record batch metadata
     * - Padding to 8-byte alignment after metadata
     * - Body data with 8-byte alignment between buffers
     *
     * @param record_batch The record batch to be measured
     * @return The total size in bytes that the serialized record batch would occupy
     */
    [[nodiscard]] SPARROW_IPC_API std::size_t
    calculate_record_batch_message_size(const sparrow::record_batch& record_batch);

    /**
     * @brief Calculates the total serialized size for a collection of record batches.
     *
     * This function computes the complete size that would be produced by serializing
     * a schema message followed by all record batch messages in the collection.
     *
     * @tparam R Range type containing sparrow::record_batch objects
     * @param record_batches Collection of record batches to be measured
     * @return The total size in bytes for the complete serialized output
     * @throws std::invalid_argument if record batches have inconsistent schemas
     */
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    [[nodiscard]] std::size_t calculate_total_serialized_size(const R& record_batches)
    {
        if (record_batches.empty())
        {
            return 0;
        }

        if (!utils::check_record_batches_consistency(record_batches))
        {
            throw std::invalid_argument("Record batches have inconsistent schemas");
        }

        // Calculate schema message size (only once)
        auto it = std::ranges::begin(record_batches);
        std::size_t total_size = calculate_schema_message_size(*it);

        // Calculate record batch message sizes
        for (const auto& record_batch : record_batches)
        {
            total_size += calculate_record_batch_message_size(record_batch);
        }

        return total_size;
    }

    void fill_body_and_get_buffers_compressed(const sparrow::arrow_proxy& arrow_proxy, std::vector<uint8_t>& body, std::vector<org::apache::arrow::flatbuf::Buffer>& flatbuf_buffers, int64_t& offset, org::apache::arrow::flatbuf::CompressionType compression_type);

    /**
     * @brief Fills the body vector with serialized data from an arrow proxy and its children.
     *
     * This function recursively processes an arrow proxy by:
     * 1. Iterating through all buffers in the proxy and appending their data to the body vector
     * 2. Adding padding bytes (zeros) after each buffer to align data to 8-byte boundaries
     * 3. Recursively processing all child proxies in the same manner
     *
     * The function ensures proper memory alignment by padding each buffer's data to the next
     * 8-byte boundary, which is typically required for efficient memory access and Arrow
     * format compliance.
     *
     * @param arrow_proxy The arrow proxy containing buffers and potential child proxies to serialize
     * @param stream The output stream where the serialized body data will be written
     */
    SPARROW_IPC_API void fill_body(const sparrow::arrow_proxy& arrow_proxy, any_output_stream& stream);

    /**
     * @brief Generates a serialized body from a record batch.
     *
     * This function iterates through all columns in the provided record batch,
     * extracts their Arrow proxy representations, and serializes them into a
     * single byte vector that forms the body of the serialized data.
     *
     * @param record_batch The record batch containing columns to be serialized
     * @param stream The output stream where the serialized body will be written
     */
    SPARROW_IPC_API void generate_body(const sparrow::record_batch& record_batch, any_output_stream& stream);

    /**
     * @brief Calculates the total size of the body section for an Arrow array.
     *
     * This function recursively computes the total size needed for all buffers
     * in an Arrow array structure, including buffers from child arrays. Each
     * buffer size is aligned to 8-byte boundaries as required by the Arrow format.
     *
     * @param arrow_proxy The Arrow array proxy containing buffers and child arrays
     * @return int64_t The total aligned size in bytes of all buffers in the array hierarchy
     */
    [[nodiscard]] SPARROW_IPC_API int64_t calculate_body_size(const sparrow::arrow_proxy& arrow_proxy);

    /**
     * @brief Calculates the total body size of a record batch by summing the body sizes of all its columns.
     *
     * This function iterates through all columns in the given record batch and accumulates
     * the body size of each column's underlying Arrow array proxy. The body size represents
     * the total memory required for the serialized data content of the record batch.
     *
     * @param record_batch The sparrow record batch containing columns to calculate size for
     * @return int64_t The total body size in bytes of all columns in the record batch
     */
    [[nodiscard]] SPARROW_IPC_API int64_t calculate_body_size(const sparrow::record_batch& record_batch);

    SPARROW_IPC_API std::vector<sparrow::data_type> get_column_dtypes(const sparrow::record_batch& rb);
}
