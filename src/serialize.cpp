#include <optional>

#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    void common_serialize(
        const flatbuffers::FlatBufferBuilder& builder,
        any_output_stream& stream
    )
    {
        stream.write(continuation);
        const flatbuffers::uoffset_t size = builder.GetSize();
        const std::span<const uint8_t> size_span(reinterpret_cast<const uint8_t*>(&size), sizeof(uint32_t));
        stream.write(size_span);
        stream.write(std::span(builder.GetBufferPointer(), size));
        stream.add_padding();
    }

    void serialize_schema_message(const sparrow::record_batch& record_batch, any_output_stream& stream)
    {
        common_serialize(get_schema_message_builder(record_batch), stream);
    }

    serialized_record_batch_info serialize_record_batch(const sparrow::record_batch& record_batch, any_output_stream& stream,
                                std::optional<CompressionType> compression,
                                std::optional<std::reference_wrapper<CompressionCache>> cache)
    {
        // Build and serialize metadata
        flatbuffers::FlatBufferBuilder builder = get_record_batch_message_builder(record_batch, compression, cache);
        const flatbuffers::uoffset_t flatbuffer_size = builder.GetSize();
        
        // Calculate metadata length: flatbuffer size + padding to 8-byte alignment
        // Note: The metadata_length in the Arrow file footer includes the size prefix (4 bytes)
        // but not the continuation bytes
        const int32_t metadata_length = static_cast<int32_t>(
            sizeof(uint32_t) + utils::align_to_8(static_cast<size_t>(flatbuffer_size))
        );
        
        // Write metadata
        common_serialize(builder, stream);
        
        // Track position before body to calculate body length
        const size_t body_start = stream.size();
        
        // Write body
        generate_body(record_batch, stream, compression, cache);
        
        // Calculate body length
        const int64_t body_length = static_cast<int64_t>(stream.size() - body_start);
        
        return {metadata_length, body_length};
    }
}
