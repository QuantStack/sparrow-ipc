#include "serialize_null_array.hpp"

namespace sparrow_ipc
{
    // A null_array is represented by metadata only (Schema, RecordBatch) and has no data buffers,
    // making its message body zero-length.
    std::vector<uint8_t> serialize_null_array(sparrow::null_array& arr)
    {
        auto [arrow_arr_ptr, arrow_schema_ptr] = sparrow::get_arrow_structures(arr);
        auto& arrow_arr = *arrow_arr_ptr;
        auto& arrow_schema = *arrow_schema_ptr;

        std::vector<uint8_t> final_buffer;
        // I - Serialize the Schema message
        details::serialize_schema_message(arrow_schema, arr.metadata(), final_buffer);

        // II - Serialize the RecordBatch message
        details::serialize_record_batch_message(arrow_arr, {}, final_buffer);

        // Return the final buffer containing the complete IPC stream
        return final_buffer;
    }

    // This reads the Schema and RecordBatch messages to extract the array's length,
    // name, and metadata, then constructs a null_array.
    sparrow::null_array deserialize_null_array(const std::vector<uint8_t>& buffer)
    {
        const uint8_t* buf_ptr = buffer.data();
        size_t current_offset = 0;

        // I - Deserialize the Schema message
        std::optional<std::string> name;
        std::optional<std::vector<sparrow::metadata_pair>> metadata;
        details::deserialize_schema_message(buf_ptr, current_offset, name, metadata);

        // II - Deserialize the RecordBatch message
        const auto* record_batch = details::deserialize_record_batch_message(buf_ptr, current_offset);

        // The body is empty, so we don't need to read any further.
        // Construct the null_array from the deserialized metadata.
        return sparrow::null_array(record_batch->length(), name, metadata);
    }
}
