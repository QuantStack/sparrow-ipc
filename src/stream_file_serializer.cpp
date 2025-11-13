#include "sparrow_ipc/stream_file_serializer.hpp"

#include <ranges>

#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/stream_file_format.hpp"

namespace sparrow_ipc
{
    stream_file_serializer::~stream_file_serializer()
    {
        if (!m_ended && m_schema_received)
        {
            // Only end if we've written something
            // Don't throw from destructor
            try
            {
                end();
            }
            catch (...)
            {
                // Swallow exceptions in destructor
            }
        }
    }

    void stream_file_serializer::write(const sparrow::record_batch& rb)
    {
        write(std::ranges::single_view(rb));
    }

    std::vector<sparrow::data_type> stream_file_serializer::get_column_dtypes(const sparrow::record_batch& rb)
    {
        std::vector<sparrow::data_type> dtypes;
        dtypes.reserve(rb.nb_columns());
        for (const auto& col : rb.columns())
        {
            dtypes.push_back(col.data_type());
        }
        return dtypes;
    }

    void stream_file_serializer::end()
    {
        if (m_ended)
        {
            return;
        }

        if (!m_schema_received)
        {
            throw std::runtime_error("Cannot end file serializer without writing any record batches");
        }

        // Write end-of-stream marker
        m_stream.write(end_of_stream);

        // Write footer using the first record batch for schema
        const size_t footer_size = write_footer(m_first_record_batch.value(), m_stream);

        // Write footer size (int32, little-endian)
        const int32_t footer_size_i32 = static_cast<int32_t>(footer_size);
        m_stream.write(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&footer_size_i32), sizeof(int32_t))
        );

        // Write magic bytes at the end
        m_stream.write(arrow_file_magic);

        m_ended = true;
    }
}
