#include "sparrow_ipc/chunk_memory_serializer.hpp"

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"

namespace sparrow_ipc
{
    chunk_serializer::chunk_serializer(chunked_memory_output_stream<std::vector<std::vector<uint8_t>>>& stream)
        : m_pstream(&stream)
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
