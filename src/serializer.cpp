#include "sparrow_ipc/serializer.hpp"

#include <ranges>

#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"

namespace sparrow_ipc
{
    serializer::serializer(const sparrow::record_batch& rb, output_stream& stream)
        : m_pstream(&stream)
        , m_dtypes(get_column_dtypes(rb))
    {
        const auto reserve_function = [&rb]()
        {
            return calculate_schema_message_size(rb) + calculate_record_batch_message_size(rb);
        };
        m_pstream->reserve(reserve_function);
        serialize_schema_message(rb, *m_pstream);
        serialize_record_batch(rb, *m_pstream);
    }

    void serializer::append(const sparrow::record_batch& rb)
    {
        if (m_ended)
        {
            throw std::runtime_error("Cannot append to a serializer that has been ended");
        }
        if (get_column_dtypes(rb) != m_dtypes)
        {
            throw std::invalid_argument("Record batch has different schema than previous ones");
        }
        const auto reserve_function = [&]()
        {
            return m_pstream->size() + calculate_record_batch_message_size(rb);
        };
        serialize_record_batch(rb, *m_pstream);
    }

    std::vector<sparrow::data_type> serializer::get_column_dtypes(const sparrow::record_batch& rb)
    {
        std::vector<sparrow::data_type> dtypes;
        dtypes.reserve(rb.nb_columns());
        for (const auto& col : rb.columns())
        {
            dtypes.push_back(col.data_type());
        }
        return dtypes;
    }

    void serializer::end()
    {
        if (m_ended)
        {
            return;
        }
        m_pstream->write(end_of_stream);
        m_pstream->flush();
        m_ended = true;
    }
}