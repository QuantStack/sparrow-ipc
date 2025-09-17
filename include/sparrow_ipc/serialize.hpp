#pragma once

#include <ostream>
#include <ranges>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "Message_generated.h"
#include "sparrow_ipc/config/config.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/utils.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Serializes a collection of record batches into a binary format.
     *
     * This function takes a collection of record batches and serializes them into a single
     * binary representation following the Arrow IPC format. The serialization includes:
     * - Schema message (derived from the first record batch)
     * - All record batch data
     * - End-of-stream marker
     *
     * @tparam R Container type that holds record batches (must support empty(), operator[], begin(), end())
     * @param record_batches Collection of record batches to serialize. All batches must have identical
     * schemas.
     *
     * @return std::vector<uint8_t> Binary serialized data containing schema, record batches, and
     * end-of-stream marker. Returns empty vector if input collection is empty.
     *
     * @throws std::invalid_argument If record batches have inconsistent schemas or if the collection
     *                               contains batches that cannot be serialized together.
     *
     * @pre All record batches in the collection must have the same schema
     * @pre The container R must not be empty when consistency checking is required
     */
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    std::vector<uint8_t> serialize(const R& record_batches)
    {
        if (record_batches.empty())
        {
            return {};
        }
        if (!utils::check_record_batches_consistency(record_batches))
        {
            throw std::invalid_argument(
                "All record batches must have the same schema to be serialized together."
            );
        }
        std::vector<uint8_t> serialized_schema = serialize_schema_message(record_batches[0]);
        std::vector<uint8_t> serialized_record_batches = serialize_record_batches(record_batches);
        serialized_schema.insert(
            serialized_schema.end(),
            std::make_move_iterator(serialized_record_batches.begin()),
            std::make_move_iterator(serialized_record_batches.end())
        );
        // End of stream message
        serialized_schema.insert(serialized_schema.end(), end_of_stream.begin(), end_of_stream.end());
        return serialized_schema;
    }
}
