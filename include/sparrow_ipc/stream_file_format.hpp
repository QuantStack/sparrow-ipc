#pragma once

#include <ranges>
#include <span>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/any_output_stream.hpp"
#include "sparrow_ipc/compression.hpp"
#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc
{
    /**
     * @brief Serializes a collection of record batches to Arrow IPC file format.
     *
     * The Arrow IPC file format consists of:
     * 1. Magic bytes "ARROW1" with padding (8 bytes)
     * 2. The stream format (schema + record batches)
     * 3. Footer containing schema and block metadata
     * 4. Footer size (int32, little-endian)
     * 5. Magic bytes "ARROW1" again (6 bytes)
     *
     * @tparam R Container type that holds record batches (must support empty(), operator[], begin(), end())
     * @param record_batches Collection of record batches to serialize. All batches must have identical schemas.
     * @param stream The output stream where the serialized file will be written.
     * @param compression The compression type to use when serializing record batches.
     *
     * @throws std::invalid_argument If record batches have inconsistent schemas or if the collection
     *                               contains batches that cannot be serialized together.
     *
     * @pre All record batches in the collection must have the same schema
     * @pre The container R must not be empty when consistency checking is required
     */
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    void serialize_to_file(const R& record_batches, any_output_stream& stream, std::optional<CompressionType> compression = std::nullopt);

    /**
     * @brief Deserializes Arrow IPC file format into a vector of record batches.
     *
     * Reads an Arrow IPC file format which consists of:
     * 1. Magic bytes "ARROW1" with padding (8 bytes)
     * 2. Stream format data (schema + record batches)
     * 3. Footer containing metadata
     * 4. Footer size (int32)
     * 5. Trailing magic bytes "ARROW1" (6 bytes)
     *
     * @param data A span of bytes containing the serialized Arrow IPC file data
     *
     * @return std::vector<sparrow::record_batch> A vector containing all deserialized record batches
     *
     * @throws std::runtime_error If:
     *         - The file magic bytes are incorrect
     *         - The footer is missing or invalid
     *         - Record batch deserialization fails
     *
     * @note The function validates the file structure including magic bytes at both start and end
     */
    [[nodiscard]] SPARROW_IPC_API std::vector<sparrow::record_batch>
    deserialize_file(std::span<const uint8_t> data);
}

#include "sparrow_ipc/stream_file_format_impl.hpp"
