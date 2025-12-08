#include "sparrow_ipc/chunk_memory_serializer.hpp"

namespace sparrow_ipc
{
    chunk_serializer::chunk_serializer(chunked_memory_output_stream<std::vector<std::vector<uint8_t>>>& stream, std::optional<CompressionType> compression)
        : m_pstream(&stream), m_compression(compression)
    {
    }

    void chunk_serializer::write(const sparrow::record_batch& rb)
    {
        write(std::ranges::single_view(rb));
    }

    void chunk_serializer::end()
    {
        if (m_ended)
        {
            return;
        }
        std::vector<uint8_t> buffer(end_of_stream.begin(), end_of_stream.end());
        m_pstream->write(std::move(buffer));
        m_ended = true;
    }
}
