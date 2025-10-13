#pragma once

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/chunk_memory_output_stream.hpp"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"

namespace sparrow_ipc
{
    class SPARROW_IPC_API chunk_serializer
    {
    public:

        chunk_serializer(chunked_memory_output_stream<std::vector<std::vector<uint8_t>>>& stream);

        void write(const sparrow::record_batch& rb);

        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        void write(const R& record_batches)
        {
            if (m_ended)
            {
                throw std::runtime_error("Cannot append to a serializer that has been ended");
            }

            m_pstream->reserve((m_schema_received ? 0 : 1) + m_pstream->size() + record_batches.size());

            if (!m_schema_received)
            {
                m_schema_received = true;
                m_dtypes = get_column_dtypes(*record_batches.begin());
                std::vector<uint8_t> schema_buffer;
                memory_output_stream stream(schema_buffer);
                any_output_stream astream(stream);
                serialize_schema_message(*record_batches.begin(), astream);
                m_pstream->write(std::move(schema_buffer));
            }

            for (const auto& rb : record_batches)
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);
                any_output_stream astream(stream);
                serialize_record_batch(rb, astream);
                m_pstream->write(std::move(buffer));
            }
        }

        /**
         * @brief Appends a record batch using the stream insertion operator.
         *
         * This operator provides a convenient stream-like interface for appending
         * record batches to the serializer. It delegates to the append() method
         * and returns a reference to the serializer to enable method chaining.
         *
         * @param rb The record batch to append to the serializer
         * @return A reference to this serializer for method chaining
         * @throws std::invalid_argument if the record batch schema doesn't match
         * @throws std::runtime_error if the serializer has been ended
         *
         * @example
         * chunk_serializer ser(initial_batch, stream);
         * ser << batch1 << batch2 << batch3;
         */
        chunk_serializer& operator<<(const sparrow::record_batch& rb)
        {
            write(rb);
            return *this;
        }

        /**
         * @brief Appends a range of record batches using the stream insertion operator.
         *
         * This operator provides a convenient stream-like interface for appending
         * multiple record batches to the serializer at once. It delegates to the
         * append() method and returns a reference to the serializer to enable method chaining.
         *
         * @tparam R The type of the record batch collection (must be an input range)
         * @param record_batches A range of record batches to append to the serializer
         * @return A reference to this serializer for method chaining
         * @throws std::invalid_argument if any record batch schema doesn't match
         * @throws std::runtime_error if the serializer has been ended
         *
         * @example
         * chunk_serializer ser(initial_batch, stream);
         * std::vector<sparrow::record_batch> batches = {batch1, batch2, batch3};
         * ser << batches << another_batch;
         */
        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        chunk_serializer& operator<<(const R& record_batches)
        {
            write(record_batches);
            return *this;
        }

        void end();

    private:

        bool m_schema_received{false};
        std::vector<sparrow::data_type> m_dtypes;
        chunked_memory_output_stream<std::vector<std::vector<uint8_t>>>* m_pstream;
        bool m_ended{false};
    };
}