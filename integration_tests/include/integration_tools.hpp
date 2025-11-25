#pragma once

#include <filesystem>
#include <span>
#include <vector>

#include <nlohmann/json.hpp>

#include <sparrow/record_batch.hpp>

namespace integration_tools
{
    /**
     * @brief Converts a JSON file to Arrow IPC file format.
     *
     * Reads a JSON file from the specified path and converts its contents into
     * Apache Arrow IPC (Inter-Process Communication) file format, returning the
     * serialized Arrow data as a byte vector.
     *
     * @param json_path The filesystem path to the input JSON file to be converted.
     * @return std::vector<uint8_t> A byte vector containing the Arrow IPC file data.
     *
     * @throws std::filesystem::filesystem_error If the file cannot be accessed or read.
     * @throws std::runtime_error If the JSON parsing or Arrow conversion fails.
     */
    std::vector<uint8_t> json_file_to_arrow_file(const std::filesystem::path& json_path);

    /**
     * @brief Reads a JSON file and converts it to Arrow IPC stream format.
     *
     * @param json_path Path to the JSON file containing record batches
     * @return Vector of bytes containing the serialized Arrow IPC stream
     * @throws std::runtime_error if the file cannot be read or parsed
     */
    std::vector<uint8_t> json_file_to_stream(const std::filesystem::path& json_path);

    /**
     * @brief Converts JSON data to Arrow IPC stream format.
     *
     * @param json_data JSON object containing record batches
     * @return Vector of bytes containing the serialized Arrow IPC stream
     * @throws std::runtime_error if the JSON cannot be parsed
     */
    std::vector<uint8_t> json_to_stream(const nlohmann::json& json_data);

    /**
     * @brief Reads an Arrow IPC stream and re-serializes it to file format.
     *
     * @param input_stream_data Binary Arrow IPC stream data
     * @return Vector of bytes containing the re-serialized Arrow IPC file format stream
     * @throws std::runtime_error if the stream cannot be deserialized
     */
    std::vector<uint8_t> stream_to_file(std::span<const uint8_t> input_stream_data);

    /**
     * @brief Validates that a JSON file and an Arrow stream contain identical data.
     *
     * @param json_path Path to the JSON file
     * @param stream_data Binary Arrow IPC stream data
     * @return true if the data matches, false otherwise
     * @throws std::runtime_error on parsing or deserialization errors
     */
    bool
    validate_json_against_stream(const std::filesystem::path& json_path, std::span<const uint8_t> stream_data);

    /**
     * @brief Compares two record batches for equality.
     *
     * @param rb1 First record batch
     * @param rb2 Second record batch
     * @param batch_idx Index of the batch (for error reporting)
     * @param verbose If true, prints detailed error messages to stderr
     * @return true if the batches are identical, false otherwise
     */
    bool compare_record_batch(
        const sparrow::record_batch& rb1,
        const sparrow::record_batch& rb2,
        size_t batch_idx,
        bool verbose = true
    );
}
