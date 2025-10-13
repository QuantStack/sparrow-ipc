#pragma once

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/chunk_memory_output_stream.hpp"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serialize.hpp"
#include "sparrow_ipc/serialize_utils.hpp"

namespace sparrow_ipc
{
    /**
     * @brief A serializer that writes record batches to chunked memory streams.
     *
     * The chunk_serializer class provides functionality to serialize Apache Arrow record batches
     * into separate memory chunks. Each record batch (and the schema) is written as an independent
     * chunk in the output stream, making it suitable for scenarios where data needs to be processed
     * or transmitted in discrete units.
     *
     * @details The serializer maintains schema consistency across all record batches:
     * - The schema is written once as the first chunk when the first record batch is processed
     * - All subsequent record batches must have the same schema
     * - Each record batch is serialized into its own independent memory chunk
     *
     * @note Once end() is called, no further record batches can be written to this serializer.
     */
    class SPARROW_IPC_API chunk_serializer
    {
    public:

        /**
         * @brief Constructs a chunk serializer with a reference to a chunked memory output stream.
         *
         * @param stream Reference to a chunked memory output stream that will receive the serialized chunks
         */
        chunk_serializer(chunked_memory_output_stream<std::vector<std::vector<uint8_t>>>& stream);

        /**
         * @brief Writes a single record batch to the chunked stream.
         *
         * This method serializes a record batch into the chunked output stream. If this is the first
         * record batch written, the schema is automatically serialized first as a separate chunk.
         *
         * @param rb The record batch to serialize
         * @throws std::runtime_error if the serializer has been ended via end()
         * @throws std::invalid_argument if the record batch schema doesn't match previously written batches
         */
        void write(const sparrow::record_batch& rb);

        /**
         * @brief Writes a range of record batches to the chunked stream.
         *
         * This template method efficiently serializes multiple record batches to the chunked output stream.
         * If this is the first write operation, the schema is automatically serialized first as a separate chunk.
         * Each record batch is then serialized into its own independent chunk.
         *
         * @tparam R The range type containing record batches (must satisfy std::ranges::input_range)
         * @param record_batches A range of record batches to serialize
         * @throws std::runtime_error if the serializer has been ended via end()
         * @throws std::invalid_argument if any record batch schema doesn't match previously written batches
         */
        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        void write(const R& record_batches);

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
        chunk_serializer& operator<<(const sparrow::record_batch& rb);

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
        chunk_serializer& operator<<(const R& record_batches);

        /**
         * @brief Finalizes the chunk serialization by writing an end-of-stream marker.
         *
         * This method signals the end of the serialization process. After calling this method,
         * no further record batches can be written to this serializer.
         *
         * @throws std::runtime_error if attempting to write after this method has been called
         */
        void end();

    private:

        bool m_schema_received{false};
        std::vector<sparrow::data_type> m_dtypes;
        chunked_memory_output_stream<std::vector<std::vector<uint8_t>>>* m_pstream;
        bool m_ended{false};
    };

    // Implementation

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    void chunk_serializer::write(const R& record_batches)
    {
        if (m_ended)
        {
            throw std::runtime_error("Cannot append record batches to a serializer that has been ended");
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

    inline chunk_serializer& chunk_serializer::operator<<(const sparrow::record_batch& rb)
    {
        write(rb);
        return *this;
    }

    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    chunk_serializer& chunk_serializer::operator<<(const R& record_batches)
    {
        write(record_batches);
        return *this;
    }
}