#pragma once

// TODO check needs of all these below
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "sparrow.hpp"

// TODO check needs of these two
#include "Message_generated.h"
#include "Schema_generated.h"

#include "serialize.hpp"
#include "utils.hpp"

namespace sparrow_ipc
{
    // TODO move to cpp if not templated
    // TODO add comments and review

    // This function serializes a sparrow::null_array into a byte vector compliant
    // with the Apache Arrow IPC Streaming Format. It mirrors the structure of
    // serialize_primitive_array but is optimized for null_array's properties.
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

    // This function deserializes a byte vector into a sparrow::null_array.
    // It reads the Schema and RecordBatch messages to extract the array's length,
    // name, and metadata, then constructs a null_array.
    sparrow::null_array deserialize_null_array(const std::vector<uint8_t>& buffer)
    {
        const uint8_t* buf_ptr = buffer.data();
        size_t current_offset = 0;

        // I - Deserialize the Schema message
        uint32_t schema_meta_len = *(reinterpret_cast<const uint32_t*>(buf_ptr + current_offset));
        current_offset += sizeof(uint32_t);
        auto schema_message = org::apache::arrow::flatbuf::GetMessage(buf_ptr + current_offset);
        if (schema_message->header_type() != org::apache::arrow::flatbuf::MessageHeader::Schema)
        {
            throw std::runtime_error("Expected Schema message at the start of the buffer.");
        }
        auto flatbuffer_schema = static_cast<const org::apache::arrow::flatbuf::Schema*>(schema_message->header());
        auto fields = flatbuffer_schema->fields();
        if (fields->size() != 1)
        {
            throw std::runtime_error("Expected schema with exactly one field for null_array.");
        }
        auto field = fields->Get(0);
        if (field->type_type() != org::apache::arrow::flatbuf::Type::Null)
        {
             throw std::runtime_error("Expected Null type in schema.");
        }

        std::optional<std::string> name;
        if (auto fb_name = field->name())
        {
            name = std::string(fb_name->c_str(), fb_name->size());
        }

        std::optional<std::vector<sparrow::metadata_pair>> metadata;
        if (auto fb_metadata = field->custom_metadata())
        {
            if (fb_metadata->size() > 0)
            {
                metadata = std::vector<sparrow::metadata_pair>();
                metadata->reserve(fb_metadata->size());
                for (const auto& kv : *fb_metadata)
                {
                    metadata->emplace_back(kv->key()->str(), kv->value()->str());
                }
            }
        }

        current_offset += schema_meta_len;

        // II - Deserialize the RecordBatch message
        uint32_t batch_meta_len = *(reinterpret_cast<const uint32_t*>(buf_ptr + current_offset));
        current_offset += sizeof(uint32_t);
        auto batch_message = org::apache::arrow::flatbuf::GetMessage(buf_ptr + current_offset);
        if (batch_message->header_type() != org::apache::arrow::flatbuf::MessageHeader::RecordBatch)
        {
            throw std::runtime_error("Expected RecordBatch message, but got a different type.");
        }
        auto record_batch = static_cast<const org::apache::arrow::flatbuf::RecordBatch*>(batch_message->header());

        // The body is empty, so we don't need to read any further.
        // Construct the null_array from the deserialized metadata.
        return sparrow::null_array(record_batch->length(), name, metadata);
    }
}
