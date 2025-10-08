#include "sparrow_ipc/chunk_memory_serializer.hpp"

#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"

namespace sparrow_ipc
{
    chunk_serializer::chunk_serializer(
        const sparrow::record_batch& rb,
        chunked_memory_output_stream<std::vector<std::vector<uint8_t>>>& stream
    )
        : m_pstream(&stream)
        , m_dtypes(get_column_dtypes(rb))
    {
        m_pstream->reserve(2);
        std::vector<uint8_t> schema_buffer;
        memory_output_stream schema_stream(schema_buffer);
        serialize_schema_message(rb, schema_stream);
        m_pstream->write(std::move(schema_buffer));

        std::vector<uint8_t> batch_buffer;
        memory_output_stream batch_stream(batch_buffer);
        serialize_record_batch(rb, batch_stream);
        m_pstream->write(std::move(batch_buffer));
    }

    void chunk_serializer::append(const sparrow::record_batch& rb)
    {
        if (m_ended)
        {
            throw std::runtime_error("Cannot append to a serializer that has been ended");
        }
        if (get_column_dtypes(rb) != m_dtypes)
        {
            throw std::invalid_argument("Record batch has different schema than previous ones");
        }
        m_pstream->reserve(m_pstream->size() + 1);
        std::vector<uint8_t> buffer;
        memory_output_stream stream(buffer);
        serialize_record_batch(rb, stream);
        m_pstream->write(std::move(buffer));
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
