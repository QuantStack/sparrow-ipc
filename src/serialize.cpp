#include <optional>

#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/flatbuffer_utils.hpp"

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

    void serialize_record_batch(const sparrow::record_batch& record_batch, any_output_stream& stream,
                                std::optional<CompressionType> compression,
                                std::optional<std::reference_wrapper<compression_cache_t>> cache)
    {
        common_serialize(get_record_batch_message_builder(record_batch, compression, cache), stream);
        generate_body(record_batch, stream, compression, cache);
    }
}
