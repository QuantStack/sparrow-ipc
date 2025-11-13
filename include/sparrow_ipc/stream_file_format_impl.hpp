#pragma once

#include <flatbuffers/flatbuffers.h>
#include <File_generated.h>

#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/utils.hpp"
#include "sparrow_ipc/magic_values.hpp"

namespace sparrow_ipc
{
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    void serialize_to_file(const R& record_batches, any_output_stream& stream, std::optional<CompressionType> compression)
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

        // Write magic bytes at the beginning of the file
        stream.write(arrow_file_header_magic);

        // Track the current position for block metadata
        int64_t current_position = static_cast<int64_t>(arrow_file_header_magic.size());

        // Store block metadata for the footer
        std::vector<org::apache::arrow::flatbuf::Block> record_batch_blocks;

        // Write schema message and track its position
        const size_t schema_start = stream.size();
        serialize_schema_message(record_batches[0], stream);
        current_position = static_cast<int64_t>(stream.size());

        // Write each record batch and track metadata
        for (const auto& rb : record_batches)
        {
            const int64_t block_offset = current_position;
            const size_t before_write = stream.size();
            
            // Get the message builder to calculate metadata size
            auto message_builder = get_record_batch_message_builder(rb, compression);
            const int32_t metadata_length = static_cast<int32_t>(message_builder.GetSize());
            const int64_t body_length = calculate_body_size(rb, compression);
            
            // Serialize the record batch
            serialize_record_batch(rb, stream, compression);
            
            // Create block metadata
            record_batch_blocks.emplace_back(
                org::apache::arrow::flatbuf::Block(block_offset, metadata_length, body_length)
            );
            
            current_position = static_cast<int64_t>(stream.size());
        }

        // Build and write footer
        flatbuffers::FlatBufferBuilder footer_builder;
        
        // Create schema for footer
        const auto fields_vec = create_children(footer_builder, record_batches[0]);
        const auto schema_offset = org::apache::arrow::flatbuf::CreateSchema(
            footer_builder,
            org::apache::arrow::flatbuf::Endianness::Little,
            fields_vec
        );
        
        // Create empty dictionaries vector
        auto dictionaries_fb = footer_builder.CreateVectorOfStructs(
            std::vector<org::apache::arrow::flatbuf::Block>{}
        );
        
        // Create record batches vector
        auto record_batches_fb = footer_builder.CreateVectorOfStructs(record_batch_blocks);
        
        // Create footer
        auto footer = org::apache::arrow::flatbuf::CreateFooter(
            footer_builder,
            org::apache::arrow::flatbuf::MetadataVersion::V5,
            schema_offset,
            dictionaries_fb,
            record_batches_fb
        );
        
        footer_builder.Finish(footer);
        
        // Write footer
        const uint8_t* footer_data = footer_builder.GetBufferPointer();
        const flatbuffers::uoffset_t footer_size = footer_builder.GetSize();
        stream.write(std::span<const uint8_t>(footer_data, footer_size));
        
        // Write footer size (int32, little-endian)
        const int32_t footer_size_i32 = static_cast<int32_t>(footer_size);
        stream.write(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(&footer_size_i32), 
            sizeof(int32_t)
        ));
        
        // Write magic bytes at the end
        stream.write(arrow_file_magic);
    }
}
