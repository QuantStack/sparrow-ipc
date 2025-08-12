#pragma once

#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "sparrow.hpp"

#include "Message_generated.h"
#include "Schema_generated.h"

#include "serialize.hpp"
#include "utils.hpp"

namespace sparrow_ipc
{
    template <typename T>
    std::vector<uint8_t> serialize_primitive_array(sparrow::primitive_array<T>& arr);

    template <typename T>
    sparrow::primitive_array<T> deserialize_primitive_array(const std::vector<uint8_t>& buffer);

    template <typename T>
    std::vector<uint8_t> serialize_primitive_array(sparrow::primitive_array<T>& arr)
    {
        // This function serializes a sparrow::primitive_array into a byte vector that is compliant
        // with the Apache Arrow IPC Streaming Format. It constructs a stream containing two messages:
        // 1. A Schema message: Describes the data's metadata (field name, type, nullability).
        // 2. A RecordBatch message: Contains the actual array data (null count, length, and raw buffers).
        // This two-part structure makes the data self-describing and readable by other Arrow-native tools.
        // The implementation adheres to the specification by correctly handling:
        // - Message order (Schema first, then RecordBatch).
        // - The encapsulated message format (4-byte metadata length prefix).
        // - 8-byte padding and alignment for the message body.
        // - Correctly populating the Flatbuffer-defined metadata for both messages.

        // Get arrow structures
        auto [arrow_arr_ptr, arrow_schema_ptr] = sparrow::get_arrow_structures(arr);
        auto& arrow_arr = *arrow_arr_ptr;
        auto& arrow_schema = *arrow_schema_ptr;

        // This will be the final buffer holding the complete IPC stream.
        std::vector<uint8_t> final_buffer;

        // I - Serialize the Schema message
        details::serialize_schema_message(arrow_schema, arr.metadata(), final_buffer);

        // II - Serialize the RecordBatch message
        // After the Schema, we send the RecordBatch containing the actual data

        // Calculate the size of the validity and data buffers
        int64_t validity_size = (arrow_arr.length + 7) / 8;
        int64_t data_size = arrow_arr.length * sizeof(T);
        std::vector<int64_t> buffers_sizes = {validity_size, data_size};
        details::serialize_record_batch_message(arrow_arr, buffers_sizes, final_buffer);

        // Return the final buffer containing the complete IPC stream
        return final_buffer;
    }

    template <typename T>
    sparrow::primitive_array<T> deserialize_primitive_array(const std::vector<uint8_t>& buffer) {
        const uint8_t* buf_ptr = buffer.data();
        size_t current_offset = 0;

        // I - Deserialize the Schema message
        std::optional<std::string> name;
        std::optional<std::vector<sparrow::metadata_pair>> metadata;
        details::deserialize_schema_message(buf_ptr, current_offset, name, metadata);

        // II - Deserialize the RecordBatch message
        uint32_t batch_meta_len = *(reinterpret_cast<const uint32_t*>(buf_ptr + current_offset));
        const auto* record_batch = details::deserialize_record_batch_message(buf_ptr, current_offset);

        current_offset += utils::align_to_8(batch_meta_len);
        const uint8_t* body_ptr = buf_ptr + current_offset;

        // Extract metadata from the RecordBatch
        auto buffers_meta = record_batch->buffers();
        auto nodes_meta = record_batch->nodes();
        auto node_meta = nodes_meta->Get(0);

        // The body contains the validity bitmap and the data buffer concatenated
        // We need to copy this data into memory owned by the new ArrowArray
        int64_t validity_len = buffers_meta->Get(0)->length();
        int64_t data_len = buffers_meta->Get(1)->length();

        uint8_t* validity_buffer_copy = new uint8_t[validity_len];
        memcpy(validity_buffer_copy, body_ptr + buffers_meta->Get(0)->offset(), validity_len);

        uint8_t* data_buffer_copy = new uint8_t[data_len];
        memcpy(data_buffer_copy, body_ptr + buffers_meta->Get(1)->offset(), data_len);


        auto data = sparrow::u8_buffer<T>(reinterpret_cast<T*>(data_buffer_copy), node_meta->length());
        auto bitmap = sparrow::validity_bitmap(validity_buffer_copy, node_meta->length());

        return sparrow::primitive_array<T>(std::move(data), node_meta->length(), std::move(bitmap), name, metadata);
    }
}
