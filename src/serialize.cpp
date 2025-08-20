#include <cstring>
#include <stdexcept>

#include "serialize.hpp"
#include "utils.hpp"

namespace sparrow_ipc
{
    namespace details
    {
        std::vector<uint8_t> serialize_schema_message(const ArrowSchema& arrow_schema)
        {
            // Create a new builder for the Schema message's metadata
            flatbuffers::FlatBufferBuilder schema_builder;

            flatbuffers::Offset<flatbuffers::String> fb_name_offset = 0;
            if (arrow_schema.name)
            {
                fb_name_offset = schema_builder.CreateString(arrow_schema.name);
            }

            // Determine the Flatbuffer type information from the C schema's format string
            const auto [type_enum, type_offset] = utils::get_flatbuffer_type(schema_builder, arrow_schema.format);

            // Handle metadata
            flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>>
                fb_metadata_offset = 0;

            if (arrow_schema.metadata)
            {
                const auto metadata_view = sparrow::key_value_view(arrow_schema.metadata);
                std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> kv_offsets;
                kv_offsets.reserve(metadata_view.size());
                for (const auto& [key, value] : metadata_view)
                {
                    const auto key_offset = schema_builder.CreateString(std::string(key));
                    const auto value_offset = schema_builder.CreateString(std::string(value));
                    kv_offsets.push_back(
                        org::apache::arrow::flatbuf::CreateKeyValue(schema_builder, key_offset, value_offset));
                }
                fb_metadata_offset = schema_builder.CreateVector(kv_offsets);
            }

            // Build the Field object
            const auto fb_field = org::apache::arrow::flatbuf::CreateField(
                schema_builder,
                fb_name_offset,
                (arrow_schema.flags & static_cast<int64_t>(sparrow::ArrowFlag::NULLABLE)) != 0,
                type_enum,
                type_offset,
                0,  // dictionary
                0,  // children
                fb_metadata_offset);

            // A Schema contains a vector of fields
            const std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> fields_vec = {fb_field};
            const auto fb_fields = schema_builder.CreateVector(fields_vec);

            // Build the Schema object from the vector of fields
            const auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(schema_builder, org::apache::arrow::flatbuf::Endianness::Little, fb_fields);

            // Wrap the Schema in a top-level Message, which is the standard IPC envelope
            const auto schema_message_offset = org::apache::arrow::flatbuf::CreateMessage(
                schema_builder,
                org::apache::arrow::flatbuf::MetadataVersion::V5,
                org::apache::arrow::flatbuf::MessageHeader::Schema,
                schema_offset.Union(),
                0
            );
            schema_builder.Finish(schema_message_offset);

            // Assemble the Schema message bytes
            const uint32_t schema_len = schema_builder.GetSize(); // Get the size of the serialized metadata
            // This will be the final buffer holding the complete IPC stream.
            std::vector<uint8_t> final_buffer;
            final_buffer.resize(sizeof(uint32_t) + schema_len); // Resize the buffer to hold the message
            // Copy the metadata into the buffer, after the 4-byte length prefix
            memcpy(final_buffer.data() + sizeof(uint32_t), schema_builder.GetBufferPointer(), schema_len);
            // Write the 4-byte metadata length at the beginning of the message
            *(reinterpret_cast<uint32_t*>(final_buffer.data())) = schema_len;
            return final_buffer;
        }

        void serialize_record_batch_message(const ArrowArray& arrow_arr, const std::vector<int64_t>& buffers_sizes, std::vector<uint8_t>& final_buffer)
        {
            // Create a new builder for the RecordBatch message's metadata
            flatbuffers::FlatBufferBuilder batch_builder;

            std::vector<org::apache::arrow::flatbuf::Buffer> buffers_vec;
            int64_t current_offset = 0;
            int64_t body_len = 0; // The total size of the message body
            for (const auto& size : buffers_sizes)
            {
                buffers_vec.emplace_back(current_offset, size);
                current_offset += size;
            }
            body_len = current_offset;

            // Create the FieldNode, which describes the layout of the array data
            const org::apache::arrow::flatbuf::FieldNode field_node_struct(arrow_arr.length, arrow_arr.null_count);
            // A RecordBatch contains a vector of nodes and a vector of buffers
            const auto fb_nodes_vector = batch_builder.CreateVectorOfStructs(&field_node_struct, 1);
            const auto fb_buffers_vector = batch_builder.CreateVectorOfStructs(buffers_vec);

            // Build the RecordBatch metadata object
            const auto record_batch_offset = org::apache::arrow::flatbuf::CreateRecordBatch(batch_builder, arrow_arr.length, fb_nodes_vector, fb_buffers_vector);

            // Wrap the RecordBatch in a top-level Message
            const auto batch_message_offset = org::apache::arrow::flatbuf::CreateMessage(
                batch_builder,
                org::apache::arrow::flatbuf::MetadataVersion::V5,
                org::apache::arrow::flatbuf::MessageHeader::RecordBatch,
                record_batch_offset.Union(),
                body_len
            );
            batch_builder.Finish(batch_message_offset);

            // Append the RecordBatch message to the final buffer
            const uint32_t batch_meta_len = batch_builder.GetSize(); // Get the size of the batch metadata
            const int64_t aligned_batch_meta_len = utils::align_to_8(batch_meta_len); // Calculate the padded length

            const size_t current_size = final_buffer.size(); // Get the current size (which is the end of the Schema message)
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
            for (size_t i = 0; i < buffers_sizes.size(); ++i)
            {
                // arrow_arr.buffers[0] is the validity bitmap
                // arrow_arr.buffers[1] is the actual data buffer
                const uint8_t* data_buffer = reinterpret_cast<const uint8_t*>(arrow_arr.buffers[i]);
                if (data_buffer)
                {
                    memcpy(dst, data_buffer, buffers_sizes[i]);
                }
                else
                {
                    // If validity_bitmap is null, it means there are no nulls
                    if (i == 0)
                    {
                        memset(dst, 0xFF, buffers_sizes[i]);
                    }
                }
                dst += buffers_sizes[i];
            }
        }

        void deserialize_schema_message(const uint8_t* buf_ptr, size_t& current_offset, std::optional<std::string>& name, std::optional<std::vector<sparrow::metadata_pair>>& metadata)
        {
            const uint32_t schema_meta_len = *(reinterpret_cast<const uint32_t*>(buf_ptr + current_offset));
            current_offset += sizeof(uint32_t);
            const auto schema_message = org::apache::arrow::flatbuf::GetMessage(buf_ptr + current_offset);
            if (schema_message->header_type() != org::apache::arrow::flatbuf::MessageHeader::Schema)
            {
                throw std::runtime_error("Expected Schema message at the start of the buffer.");
            }
            const auto flatbuffer_schema = static_cast<const org::apache::arrow::flatbuf::Schema*>(schema_message->header());
            const auto fields = flatbuffer_schema->fields();
            if (fields->size() != 1)
            {
                throw std::runtime_error("Expected schema with exactly one field.");
            }

            const auto field = fields->Get(0);

            // Get name
            if (const auto fb_name = field->name())
            {
                name = fb_name->str();
            }

            // Handle metadata
            const auto fb_metadata = field->custom_metadata();
            if (fb_metadata && !fb_metadata->empty())
            {
                metadata = std::vector<sparrow::metadata_pair>();
                metadata->reserve(fb_metadata->size());
                for (const auto& kv : *fb_metadata)
                {
                    metadata->emplace_back(kv->key()->str(), kv->value()->str());
                }
            }
            current_offset += schema_meta_len;
        }

        const org::apache::arrow::flatbuf::RecordBatch* deserialize_record_batch_message(const uint8_t* buf_ptr, size_t& current_offset)
        {
            current_offset += sizeof(uint32_t);
            const auto batch_message = org::apache::arrow::flatbuf::GetMessage(buf_ptr + current_offset);
            if (batch_message->header_type() != org::apache::arrow::flatbuf::MessageHeader::RecordBatch)
            {
                throw std::runtime_error("Expected RecordBatch message, but got a different type.");
            }
            return static_cast<const org::apache::arrow::flatbuf::RecordBatch*>(batch_message->header());
        }

    } // namespace details
} // namespace sparrow-ipc
