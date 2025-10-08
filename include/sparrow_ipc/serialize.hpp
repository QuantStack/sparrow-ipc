#pragma once

#include <ranges>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/output_stream.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Serializes a collection of record batches into a binary format.
     *
     * This function takes a collection of record batches and serializes them into a single
     * binary representation following the Arrow IPC format. The serialization includes:
     * - Schema message (derived from the first record batch)
     * - All record batch data
     * - End-of-stream marker
     *
     * @tparam R Container type that holds record batches (must support empty(), operator[], begin(), end())
     * @param record_batches Collection of record batches to serialize. All batches must have identical
     * schemas.
     * @param stream The output stream where the serialized data will be written.
     *
     * @throws std::invalid_argument If record batches have inconsistent schemas or if the collection
     *                               contains batches that cannot be serialized together.
     *
     * @pre All record batches in the collection must have the same schema
     * @pre The container R must not be empty when consistency checking is required
     */
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    void serialize_record_batches_to_ipc_stream(const R& record_batches, output_stream& stream)
    {
        if (record_batches.empty())
        {
            return;
        }

        if (!utils::check_record_batches_consistency(record_batches))
        {
            throw std::invalid_argument(
                "All record batches must have the same schema to be serialized together."
            );
        }
        serialize_schema_message(record_batches[0], stream);
        for (const auto& rb : record_batches)
        {
            serialize_record_batch(rb, stream);
        }
        stream.write(end_of_stream);
    }

    /**
     * @brief Serializes a record batch into a binary format following the Arrow IPC specification.
     *
     * This function converts a sparrow record batch into a serialized byte vector that includes:
     * - A continuation marker
     * - The record batch message length (4 bytes)
     * - The flatbuffer-encoded record batch metadata
     * - Padding to align to 8-byte boundaries
     * - The record batch body containing the actual data buffers
     *
     * @param record_batch The sparrow record batch to serialize
     * @param stream The output stream where the serialized record batch will be written
     *
     * @note The output follows Arrow IPC message format with proper alignment and
     *       includes both metadata and data portions of the record batch
     */
    SPARROW_IPC_API void
    serialize_record_batch(const sparrow::record_batch& record_batch, output_stream& stream);

    /**
     * @brief Serializes a schema message for a record batch into a byte buffer.
     *
     * This function creates a serialized schema message following the Arrow IPC format.
     * The resulting buffer contains:
     * 1. Continuation bytes at the beginning
     * 2. A 4-byte length prefix indicating the size of the schema message
     * 3. The actual FlatBuffer schema message bytes
     * 4. Padding bytes to align the total size to 8-byte boundaries
     *
     * @param record_batch The record batch containing the schema to serialize
     * @param stream The output stream where the serialized schema message will be written
     */
    SPARROW_IPC_API void
    serialize_schema_message(const sparrow::record_batch& record_batch, output_stream& stream);

}
