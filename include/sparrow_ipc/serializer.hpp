#include <cstddef>
#include <numeric>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/output_stream.hpp"
#include "sparrow_ipc/serialize_utils.hpp"

namespace sparrow_ipc
{
    /**
     * @brief A class for serializing Apache Arrow record batches to an output stream.
     *
     * The serializer class provides functionality to serialize single or multiple record batches
     * into a binary format suitable for storage or transmission. It ensures schema consistency
     * across multiple record batches and optimizes memory allocation by pre-calculating required
     * buffer sizes.
     *
     * @details The serializer supports two main usage patterns:
     * 1. Construction with a collection of record batches for batch serialization
     * 2. Construction with a single record batch followed by incremental appends
     *
     * The class validates that all record batches have consistent schemas and throws
     * std::invalid_argument if inconsistencies are detected or if an empty collection
     * is provided.
     *
     * Memory efficiency is achieved through:
     * - Pre-calculation of total serialization size
     * - Stream reservation to minimize memory reallocations
     * - Lazy evaluation of size calculations using lambda functions
     */
    class SPARROW_IPC_API serializer
    {
    public:

        serializer(const sparrow::record_batch& rb, output_stream& stream);

        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        serializer(const R& record_batches, output_stream& stream)
            : m_pstream(&stream)
            , m_dtypes(get_column_dtypes(record_batches[0]))
        {
            if (record_batches.empty())
            {
                throw std::invalid_argument("Record batches collection is empty");
            }

            const auto reserve_function = [&record_batches]()
            {
                return calculate_schema_message_size(record_batches[0])
                       + std::accumulate(
                           record_batches.cbegin(),
                           record_batches.cend(),
                           0,
                           [](size_t acc, const sparrow::record_batch& rb)
                           {
                               return acc + calculate_record_batch_message_size(rb);
                           }
                       );
            };
            m_pstream->reserve(reserve_function);
            serialize_schema_message(record_batches[0], *m_pstream);
            append(record_batches);
        }

        /**
         * Appends a record batch to the serializer.
         *
         * @param rb The record batch to append to the serializer
         */
        void append(const sparrow::record_batch& rb);

        /**
         * @brief Appends a collection of record batches to the stream.
         *
         * This method efficiently adds multiple record batches to the serialization stream
         * by first calculating the total required size and reserving memory space to minimize
         * reallocations during the append operations.
         *
         * @tparam R The type of the record batch collection (must be iterable)
         * @param record_batches A collection of record batches to append to the stream
         *
         * The method performs the following operations:
         * 1. Calculates the total size needed for all record batches
         * 2. Reserves the required memory space in the stream
         * 3. Iterates through each record batch and adds it to the stream
         */
        template <std::ranges::input_range R>
            requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
        void append(const R& record_batches)
        {
            if (m_ended)
            {
                throw std::runtime_error("Cannot append to a serializer that has been ended");
            }
            const auto reserve_function = [&record_batches, this]()
            {
                return std::accumulate(
                    record_batches.cbegin(),
                    record_batches.cend(),
                    m_pstream->size(),
                    [this](size_t acc, const sparrow::record_batch& rb)
                    {
                        return acc + calculate_record_batch_message_size(rb);
                    }
                );
            };
            m_pstream->reserve(reserve_function);
            for (const auto& rb : record_batches)
            {
                serialize_record_batch(rb, *m_pstream);
            }
        }

        /**
         * @brief Finalizes the serialization process by writing end-of-stream marker.
         *
         * This method writes an end-of-stream marker to the output stream and flushes
         * any buffered data. It can be called multiple times safely as it tracks
         * whether the stream has already been ended to prevent duplicate operations.
         *
         * @note This method is idempotent - calling it multiple times has no additional effect.
         * @post After calling this method, m_ended will be set to true.
         */
        void end();

    private:

        static std::vector<sparrow::data_type> get_column_dtypes(const sparrow::record_batch& rb);

        std::vector<sparrow::data_type> m_dtypes;
        output_stream* m_pstream;
        bool m_ended{false};
    };
}