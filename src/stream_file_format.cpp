#include "sparrow_ipc/stream_file_format.hpp"

#include <cstring>
#include <stdexcept>

#include <File_generated.h>

#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/magic_values.hpp"

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

        // Calculate the end of the stream data (before footer)
        const size_t footer_offset = footer_size_offset - footer_size;
        
        // Extract the stream portion (from after header magic to before footer)
        // Stream data starts after the 8-byte header magic
        const size_t stream_start = arrow_file_header_magic.size();
        const size_t stream_length = footer_offset - stream_start;
        
        auto stream_data = data.subspan(stream_start, stream_length);
        
        // Use deserialize_stream to parse the stream format data
        // This handles schema message, record batches, and end-of-stream marker
        return deserialize_stream(stream_data);
    }

    size_t write_footer(
        const sparrow::record_batch& record_batch,
        any_output_stream& stream
    )
    {
        // Build footer using FlatBufferBuilder
        flatbuffers::FlatBufferBuilder footer_builder;

        // Create schema for footer
        const auto fields_vec = create_children(footer_builder, record_batch);
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
        std::vector<org::apache::arrow::flatbuf::Block> record_batch_blocks;
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
        return footer_size;
    }
}
