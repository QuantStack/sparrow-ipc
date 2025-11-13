#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"
#include "Schema_generated.h"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Deserializes arrays from an Apache Arrow RecordBatch using the provided schema.
     *
     * This function processes each field in the schema and deserializes the corresponding
     * data from the RecordBatch into sparrow::array objects. It handles various Arrow data
     * types including primitive types (bool, integers, floating point), binary data, and
     * string data with their respective size variants.
     *
     * @param record_batch The Apache Arrow FlatBuffer RecordBatch containing the serialized data
     * @param schema The Apache Arrow FlatBuffer Schema defining the structure and types of the data
     * @param encapsulated_message The message containing the binary data buffers
     * @param field_metadata Metadata for each field
     *
     * @return std::vector<sparrow::array> A vector of deserialized arrays, one for each field in the schema
     *
     * @throws std::runtime_error If an unsupported data type, integer bit width, or floating point precision
     * is encountered
     *
     * The function maintains a buffer index that is incremented as it processes each field
     * to correctly map data buffers to their corresponding arrays.
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<sparrow::array> get_arrays_from_record_batch(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        const org::apache::arrow::flatbuf::Schema& schema,
        const encapsulated_message& encapsulated_message,
        const std::vector<std::optional<std::vector<sparrow::metadata_pair>>>& field_metadata
    );

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