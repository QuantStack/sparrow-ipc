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

    void serialize_record_batch(const sparrow::record_batch& record_batch, any_output_stream& stream, std::optional<org::apache::arrow::flatbuf::CompressionType> compression)
    {
        if (compression.has_value())
        {
            // TODO Handle this inside get_record_batch_message_builder
            auto [compressed_body, compressed_buffers] = generate_compressed_body_and_buffers(record_batch, compression.value());
            common_serialize(get_record_batch_message_builder(record_batch, compression, compressed_body.size(), &compressed_buffers), stream);
            // TODO Use something equivalent to generate_body (stream wise, handling children etc)
            stream.write(std::span(compressed_body.data(), compressed_body.size()));
        }
        else
        {
            common_serialize(get_record_batch_message_builder(record_batch, compression), stream);
            generate_body(record_batch, stream);
        }
    }
}
