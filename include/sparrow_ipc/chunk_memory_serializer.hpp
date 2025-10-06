#pragma once

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/chunk_memory_output_stream.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    class SPARROW_IPC_API chunk_serializer
    {
    public:

        chunk_serializer(
            const sparrow::record_batch& rb,
            chuncked_memory_output_stream<std::vector<std::vector<uint8_t>>>& stream
        );

        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        chunk_serializer(
            const R& record_batches,
            chuncked_memory_output_stream<std::vector<std::vector<uint8_t>>>& stream
        )
            : m_pstream(&stream)
        {
            if (record_batches.empty())
            {
                throw std::invalid_argument("Record batches collection is empty");
            }
            m_dtypes = get_column_dtypes(record_batches[0]);

            m_pstream->reserve(record_batches.size() + 1);
            std::vector<uint8_t> buffer;
            memory_output_stream schema_stream(buffer);
            serialize_schema_message(record_batches[0], schema_stream);
            m_pstream->write(std::move(buffer));
            append(record_batches);
        }

        void append(const sparrow::record_batch& rb);

        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        void append(const R& record_batches)
        {
            if (m_ended)
            {
                throw std::runtime_error("Cannot append to a serializer that has been ended");
            }

            m_pstream->reserve(m_pstream->size() + record_batches.size());

            for (const auto& rb : record_batches)
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);
                serialize_record_batch(rb, stream);
                m_pstream->write(std::move(buffer));
            }
        }

        void end();

    private:

        std::vector<sparrow::data_type> m_dtypes;
        chuncked_memory_output_stream<std::vector<std::vector<uint8_t>>>* m_pstream;
        bool m_ended{false};
    };
}