#include "sparrow_ipc/serializer.hpp"


#include <ranges>
#include "sparrow_ipc/magic_values.hpp"

namespace sparrow_ipc
{
    serializer::~serializer()
    {
        if (!m_ended)
        {
            end();
        }
    }

    void serializer::write(const sparrow::record_batch& rb)
    {
        write(std::ranges::single_view(rb));
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
        m_stream.write(end_of_stream);
        m_ended = true;
    }
}