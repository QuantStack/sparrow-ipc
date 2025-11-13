#include "sparrow_ipc/stream_file_format.hpp"

#include <cstring>
#include <stdexcept>

#include <File_generated.h>

#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{    
    std::vector<sparrow::record_batch> deserialize_file(std::span<const uint8_t> data)
    {
        // Validate minimum file size
        // Magic (8) + Footer size (4) + Magic (6) = 18 bytes minimum
        constexpr size_t min_file_size = 18;
        if (data.size() < min_file_size)
        {
            throw std::runtime_error("File is too small to be a valid Arrow file");
        }

        // Check magic bytes at the beginning
        if (!is_arrow_file_magic(data.subspan(0, arrow_file_magic_size)))
        {
            throw std::runtime_error("Invalid Arrow file: missing or incorrect magic bytes at start");
        }

        // Check magic bytes at the end
        const size_t trailing_magic_offset = data.size() - arrow_file_magic_size;
        if (!is_arrow_file_magic(data.subspan(trailing_magic_offset, arrow_file_magic_size)))
        {
            throw std::runtime_error("Invalid Arrow file: missing or incorrect magic bytes at end");
        }

        // Read footer size (4 bytes before the trailing magic)
        const size_t footer_size_offset = data.size() - arrow_file_magic_size - sizeof(int32_t);
        int32_t footer_size = 0;
        std::memcpy(&footer_size, data.data() + footer_size_offset, sizeof(int32_t));

        if (footer_size <= 0 || static_cast<size_t>(footer_size) > data.size() - min_file_size)
        {
            throw std::runtime_error("Invalid footer size in Arrow file");
        }

        // Read and parse footer
        const size_t footer_offset = footer_size_offset - footer_size;
        const auto* footer = org::apache::arrow::flatbuf::GetFooter(data.data() + footer_offset);

        if (footer == nullptr)
        {
            throw std::runtime_error("Failed to parse Arrow file footer");
        }

        // Validate footer has schema
        const auto* schema = footer->schema();
        if (schema == nullptr)
        {
            throw std::runtime_error("Arrow file footer missing schema");
        }

        // Get record batch blocks
        const auto* record_batch_blocks = footer->recordBatches();
        if (record_batch_blocks == nullptr || record_batch_blocks->size() == 0)
        {
            // No record batches - return empty vector
            return std::vector<sparrow::record_batch>{};
        }

        // Extract field metadata from schema for later use
        std::vector<std::string> field_names;
        std::vector<bool> fields_nullable;
        std::vector<std::optional<std::vector<sparrow::metadata_pair>>> fields_metadata;

        if (schema->fields() != nullptr)
        {
            const size_t num_fields = schema->fields()->size();
            field_names.reserve(num_fields);
            fields_nullable.reserve(num_fields);
            fields_metadata.reserve(num_fields);

            for (const auto* field : *(schema->fields()))
            {
                if (field != nullptr && field->name() != nullptr)
                {
                    field_names.emplace_back(field->name()->str());
                }
                else
                {
                    field_names.emplace_back("_unnamed_");
                }
                fields_nullable.push_back(field != nullptr && field->nullable());

                if (field != nullptr && field->custom_metadata() != nullptr)
                {
                    fields_metadata.push_back(to_sparrow_metadata(*(field->custom_metadata())));
                }
                else
                {
                    fields_metadata.push_back(std::nullopt);
                }
            }
        }

        // Deserialize each record batch
        std::vector<sparrow::record_batch> result;
        result.reserve(record_batch_blocks->size());

        for (const auto* block : *record_batch_blocks)
        {
            if (block == nullptr)
            {
                throw std::runtime_error("Null block in record batch list");
            }

            const int64_t block_offset = block->offset();
            const int32_t metadata_length = block->metaDataLength();
            const int64_t body_length = block->bodyLength();

            // Validate block offset and lengths
            if (block_offset < 0 || metadata_length < 0 || body_length < 0)
            {
                throw std::runtime_error("Invalid block metadata: negative offset or length");
            }

            // The block offset points to the start of the message (after continuation bytes in the stream format)
            // In the file format, messages are in stream format, so continuation bytes are included
            const size_t message_start = static_cast<size_t>(block_offset);
            
            // Validate we have enough data
            if (message_start + sizeof(uint32_t) + sizeof(int32_t) > data.size())
            {
                throw std::runtime_error("Record batch block extends beyond file size");
            }

            // Extract the encapsulated message (includes continuation bytes)
            auto message_span = data.subspan(message_start);
            const auto [encapsulated_message, rest] = extract_encapsulated_message(message_span);
            
            const auto* message = encapsulated_message.flat_buffer_message();
            if (message == nullptr)
            {
                throw std::runtime_error("Failed to parse record batch message");
            }

            if (message->header_type() != org::apache::arrow::flatbuf::MessageHeader::RecordBatch)
            {
                throw std::runtime_error("Expected RecordBatch message in block");
            }

            const auto* record_batch_fb = message->header_as_RecordBatch();
            if (record_batch_fb == nullptr)
            {
                throw std::runtime_error("Failed to get RecordBatch from message");
            }

            // Deserialize arrays from this record batch
            std::vector<sparrow::array> arrays = get_arrays_from_record_batch(
                *record_batch_fb,
                *schema,
                encapsulated_message,
                fields_metadata
            );

            // Create and store the record batch
            auto names_copy = field_names;  // TODO: Remove when issue with to_vector is fixed
            result.emplace_back(std::move(names_copy), std::move(arrays));
        }

        return result;
    }
}
