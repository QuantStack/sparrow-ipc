#pragma once

#include <optional>
#include <string>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Deserializes a schema message from Arrow IPC format data.
     *
     * This function parses an Arrow IPC schema message from a byte buffer, extracting
     * the field name and custom metadata from the first (and expected only) field in the schema.
     *
     * @param data A span containing the raw byte data to deserialize from
     * @param current_offset Reference to the current position in the data buffer, which will be
     *                      updated to point past the processed schema message
     * @param name Optional output parameter that will contain the field name if present
     * @param metadata Optional output parameter that will contain the custom metadata
     *                key-value pairs if present
     *
     * @throws std::runtime_error If the message is not a Schema message type
     * @throws std::runtime_error If the schema does not contain exactly one field
     *
     * @note This function expects the data to start with a 4-byte length prefix followed
     *       by the FlatBuffer schema message data
     */
    SPARROW_IPC_API void deserialize_schema_message(
        std::span<const uint8_t> data,
        size_t& current_offset,
        std::optional<std::string>& name,
        std::optional<std::vector<sparrow::metadata_pair>>& metadata
    );
    [[nodiscard]] SPARROW_IPC_API const org::apache::arrow::flatbuf::RecordBatch*
    deserialize_record_batch_message(std::span<const uint8_t> data, size_t& current_offset);

    /**
     * @brief Deserializes an Arrow IPC stream from binary data into a vector of record batches.
     *
     * This function processes an Arrow IPC stream format, extracting schema information
     * and record batch data. It handles encapsulated messages sequentially, first expecting
     * a Schema message followed by one or more RecordBatch messages.
     *
     * @param data A span of bytes containing the serialized Arrow IPC stream data
     *
     * @return std::vector<sparrow::record_batch> A vector containing all deserialized record batches
     *
     * @throws std::runtime_error If:
     *         - A RecordBatch message is encountered before a Schema message
     *         - A RecordBatch message header is missing or invalid
     *         - Unsupported message types are encountered (Tensor, DictionaryBatch, SparseTensor)
     *         - An unknown message header type is encountered
     *
     * @note The function processes messages until an end-of-stream marker is detected
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<sparrow::record_batch>
    deserialize_stream(std::span<const uint8_t> data);
}