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
        // Use the Arrow C Data Interface to get a generic description of the array.
        // For a null_array, the ArrowArray struct will report n_buffers = 0.
        auto [arrow_arr_ptr, arrow_schema_ptr] = sparrow::get_arrow_structures(arr);
        auto& arrow_arr = *arrow_arr_ptr;
        auto& arrow_schema = *arrow_schema_ptr;

        std::vector<uint8_t> final_buffer;

        // I - Serialize the Schema message
        // This part is almost identical to how a primitive_array's schema is serialized.
        {
            flatbuffers::FlatBufferBuilder schema_builder;

            flatbuffers::Offset<flatbuffers::String> fb_name_offset = 0;
            if (arrow_schema.name)
            {
                fb_name_offset = schema_builder.CreateString(arrow_schema.name);
            }

            // For null_array, the format string is "n".
            auto [type_enum, type_offset] = utils::get_flatbuffer_type(schema_builder, arrow_schema.format);

            flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
                fb_metadata_offset = 0;

            if (arr.metadata())
            {
                sparrow::key_value_view metadata_view = *(arr.metadata());
                std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> kv_offsets;
//                 kv_offsets.reserve(metadata_view.size());
//                 for (const auto& pair : metadata_view)
//                 {
//                     auto key_offset = schema_builder.CreateString(std::string(pair.first));
//                     auto value_offset = schema_builder.CreateString(std::string(pair.second));
//                     kv_offsets.push_back(
//                         org::apache::arrow::flatbuf::CreateKeyValue(schema_builder, key_offset, value_offset));
//                 }
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

            auto fb_field = org::apache::arrow::flatbuf::CreateField(
                schema_builder,
                fb_name_offset,
                (arrow_schema.flags & static_cast<int64_t>(sparrow::ArrowFlag::NULLABLE)) != 0,
                type_enum,
                type_offset,
                0, // dictionary
                0, // children
                fb_metadata_offset);

            std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> fields_vec = {fb_field};
            auto fb_fields = schema_builder.CreateVector(fields_vec);

            auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(schema_builder, org::apache::arrow::flatbuf::Endianness::Little, fb_fields);

            auto schema_message_offset = org::apache::arrow::flatbuf::CreateMessage(
                schema_builder,
                org::apache::arrow::flatbuf::MetadataVersion::V5,
                org::apache::arrow::flatbuf::MessageHeader::Schema,
                schema_offset.Union(),
                0 // bodyLength
            );
            schema_builder.Finish(schema_message_offset);

            uint32_t schema_len = schema_builder.GetSize();
            final_buffer.resize(sizeof(uint32_t) + schema_len);
            memcpy(final_buffer.data() + sizeof(uint32_t), schema_builder.GetBufferPointer(), schema_len);
            *(reinterpret_cast<uint32_t*>(final_buffer.data())) = schema_len;
        }

        // II - Serialize the RecordBatch message
        {
            flatbuffers::FlatBufferBuilder batch_builder;

            // The FieldNode describes the layout (length and null count).
            // For a null_array, length and null_count are always equal.
            org::apache::arrow::flatbuf::FieldNode field_node_struct(arrow_arr.length, arrow_arr.null_count);
            auto fb_nodes_vector = batch_builder.CreateVectorOfStructs(&field_node_struct, 1);

            // A null_array has no buffers. The ArrowArray struct reports n_buffers = 0,
            // so we create an empty vector of buffers for the Flatbuffers message.
            auto fb_buffers_vector = batch_builder.CreateVectorOfStructs<org::apache::arrow::flatbuf::Buffer>({});

            auto record_batch_offset = org::apache::arrow::flatbuf::CreateRecordBatch(batch_builder, arrow_arr.length, fb_nodes_vector, fb_buffers_vector);

            // The bodyLength is 0 because there are no data buffers.
            auto batch_message_offset = org::apache::arrow::flatbuf::CreateMessage(
                batch_builder,
                org::apache::arrow::flatbuf::MetadataVersion::V5,
                org::apache::arrow::flatbuf::MessageHeader::RecordBatch,
                record_batch_offset.Union(),
                0 // bodyLength
            );
            batch_builder.Finish(batch_message_offset);

            uint32_t batch_meta_len = batch_builder.GetSize();
            int64_t aligned_batch_meta_len = utils::align_to_8(batch_meta_len);

            size_t current_size = final_buffer.size();
            // Resize for the RecordBatch metadata. There is no body to append.
            final_buffer.resize(current_size + sizeof(uint32_t) + aligned_batch_meta_len);
            uint8_t* dst = final_buffer.data() + current_size;

            *(reinterpret_cast<uint32_t*>(dst)) = batch_meta_len;
            dst += sizeof(uint32_t);
            memcpy(dst, batch_builder.GetBufferPointer(), batch_meta_len);
            memset(dst + batch_meta_len, 0, aligned_batch_meta_len - batch_meta_len);
        }

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
