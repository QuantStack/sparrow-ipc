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
        // An Arrow IPC stream must start with a Schema message
        {
            // Create a new builder for the Schema message's metadata
            flatbuffers::FlatBufferBuilder schema_builder;

            // Create the Field metadata, which describes a single column (or array)
            flatbuffers::Offset<flatbuffers::String> fb_name_offset = 0;
            if (arrow_schema.name)
            {
                fb_name_offset = schema_builder.CreateString(arrow_schema.name);
            }

            // Determine the Flatbuffer type information from the C schema's format string
            auto [type_enum, type_offset] = utils::get_flatbuffer_type(schema_builder, arrow_schema.format);

            // Handle metadata
            flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
                fb_metadata_offset = 0;

            if (arr.metadata())
            {
                sparrow::key_value_view metadata_view = *(arr.metadata());
                std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> kv_offsets;
                kv_offsets.reserve(metadata_view.size());
                auto mv_it = metadata_view.cbegin();
                for (auto i = 0; i < metadata_view.size(); ++i, ++mv_it)
                {
                    auto key_offset = schema_builder.CreateString(std::string((*mv_it).first));
                    auto value_offset = schema_builder.CreateString(std::string((*mv_it).second));
                    kv_offsets.push_back(
                        org::apache::arrow::flatbuf::CreateKeyValue(schema_builder, key_offset, value_offset));
                }
                fb_metadata_offset = schema_builder.CreateVector(kv_offsets);
            }

            // Build the Field object
            auto fb_field = org::apache::arrow::flatbuf::CreateField(
                schema_builder,
                fb_name_offset,
                (arrow_schema.flags & static_cast<int64_t>(sparrow::ArrowFlag::NULLABLE)) != 0,
                type_enum,
                type_offset,
                0,  // dictionary
                0,  // children
                fb_metadata_offset);

            // A Schema contains a vector of fields. For this primitive array, there is only one
            std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> fields_vec = {fb_field};
            auto fb_fields = schema_builder.CreateVector(fields_vec);

            // Build the Schema object from the vector of fields
            auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(schema_builder, org::apache::arrow::flatbuf::Endianness::Little, fb_fields);

            // Wrap the Schema in a top-level Message, which is the standard IPC envelope
            auto schema_message_offset = org::apache::arrow::flatbuf::CreateMessage(
                schema_builder,
                org::apache::arrow::flatbuf::MetadataVersion::V5,
                org::apache::arrow::flatbuf::MessageHeader::Schema,
                schema_offset.Union(),
                0
            );
            schema_builder.Finish(schema_message_offset);

            // Assemble the Schema message bytes
            uint32_t schema_len = schema_builder.GetSize(); // Get the size of the serialized metadata
            final_buffer.resize(sizeof(uint32_t) + schema_len); // Resize the buffer to hold the message
            // Copy the metadata into the buffer, after the 4-byte length prefix
            memcpy(final_buffer.data() + sizeof(uint32_t), schema_builder.GetBufferPointer(), schema_len);
            // Write the 4-byte metadata length at the beginning of the message
            *(reinterpret_cast<uint32_t*>(final_buffer.data())) = schema_len;
        }

        // II - Serialize the RecordBatch message
        // After the Schema, we send the RecordBatch containing the actual data
        {
            // Create a new builder for the RecordBatch message's metadata
            flatbuffers::FlatBufferBuilder batch_builder;

            // arrow_arr.buffers[0] is the validity bitmap
            // arrow_arr.buffers[1] is the data buffer
            const uint8_t* validity_bitmap = reinterpret_cast<const uint8_t*>(arrow_arr.buffers[0]);
            const uint8_t* data_buffer = reinterpret_cast<const uint8_t*>(arrow_arr.buffers[1]);

            // Calculate the size of the validity and data buffers
            int64_t validity_size = (arrow_arr.length + 7) / 8;
            int64_t data_size = arrow_arr.length * sizeof(T);
            int64_t body_len = validity_size + data_size; // The total size of the message body

            // Create Flatbuffer descriptions for the data buffers
            org::apache::arrow::flatbuf::Buffer validity_buffer_struct(0, validity_size);
            org::apache::arrow::flatbuf::Buffer data_buffer_struct(validity_size, data_size);
            // Create the FieldNode, which describes the layout of the array data
            org::apache::arrow::flatbuf::FieldNode field_node_struct(arrow_arr.length, arrow_arr.null_count);

            // A RecordBatch contains a vector of nodes and a vector of buffers
            auto fb_nodes_vector = batch_builder.CreateVectorOfStructs(&field_node_struct, 1);
            std::vector<org::apache::arrow::flatbuf::Buffer> buffers_vec = {validity_buffer_struct, data_buffer_struct};
            auto fb_buffers_vector = batch_builder.CreateVectorOfStructs(buffers_vec);

            // Build the RecordBatch metadata object
            auto record_batch_offset = org::apache::arrow::flatbuf::CreateRecordBatch(batch_builder, arrow_arr.length, fb_nodes_vector, fb_buffers_vector);

            // Wrap the RecordBatch in a top-level Message
            auto batch_message_offset = org::apache::arrow::flatbuf::CreateMessage(
                batch_builder,
                org::apache::arrow::flatbuf::MetadataVersion::V5,
                org::apache::arrow::flatbuf::MessageHeader::RecordBatch,
                record_batch_offset.Union(),
                body_len
            );
            batch_builder.Finish(batch_message_offset);

            // III - Append the RecordBatch message to the final buffer
            uint32_t batch_meta_len = batch_builder.GetSize(); // Get the size of the batch metadata
            int64_t aligned_batch_meta_len = utils::align_to_8(batch_meta_len); // Calculate the padded length

            size_t current_size = final_buffer.size(); // Get the current size (which is the end of the Schema message)
            // Resize the buffer to append the new message
            final_buffer.resize(current_size + sizeof(uint32_t) + aligned_batch_meta_len + body_len);
            uint8_t* dst = final_buffer.data() + current_size; // Get a pointer to where the new message will start

            // Write the 4-byte metadata length for the RecordBatch message
            *(reinterpret_cast<uint32_t*>(dst)) = batch_meta_len;
            dst += sizeof(uint32_t);
            // Copy the RecordBatch metadata into the buffer
            memcpy(dst, batch_builder.GetBufferPointer(), batch_meta_len);
            // Add padding to align the body to an 8-byte boundary
            memset(dst + batch_meta_len, 0, aligned_batch_meta_len - batch_meta_len);
            dst += aligned_batch_meta_len;
            // Copy the actual data buffers (the message body) into the buffer
            if (validity_bitmap)
            {
                memcpy(dst, validity_bitmap, validity_size);
            }
            else
            {
                // If validity_bitmap is null, it means there are no nulls
                memset(dst, 0xFF, validity_size);
            }
            dst += validity_size;
            if (data_buffer)
            {
                memcpy(dst, data_buffer, data_size);
            }
        }

        // Return the final buffer containing the complete IPC stream
        return final_buffer;
    }

    template <typename T>
    sparrow::primitive_array<T> deserialize_primitive_array(const std::vector<uint8_t>& buffer) {
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
            throw std::runtime_error("Expected schema with exactly one field for primitive_array.");
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

        // Get name
        std::optional<std::string> name;
        const flatbuffers::String* fb_name_flatbuffer = fields->Get(0)->name();
        if (fb_name_flatbuffer)
        {
            name = std::string(fb_name_flatbuffer->c_str(), fb_name_flatbuffer->size());
        }

        // Handle metadata
        std::optional<std::vector<sparrow::metadata_pair>> metadata;
        auto fb_metadata = fields->Get(0)->custom_metadata();
        if (fb_metadata && !fb_metadata->empty())
        {
            metadata = std::vector<sparrow::metadata_pair>();
            metadata->reserve(fb_metadata->size());
            for (const auto& kv : *fb_metadata)
            {
                metadata->emplace_back(kv->key()->c_str(), kv->value()->c_str());
            }
        }

        auto data = sparrow::u8_buffer<T>(reinterpret_cast<T*>(data_buffer_copy), node_meta->length());
        auto bitmap = sparrow::validity_bitmap(validity_buffer_copy, node_meta->length());

        return sparrow::primitive_array<T>(std::move(data), node_meta->length(), std::move(bitmap), name, metadata);
    }
}
