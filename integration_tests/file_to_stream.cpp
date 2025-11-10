#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include <nlohmann/json.hpp>
#include <sparrow/record_batch.hpp>

#include "sparrow/json_reader/json_parser.hpp"

#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>

/**
 * @brief Reads a JSON file containing record batches and outputs the serialized Arrow IPC stream to stdout.
 *
 * This program takes a JSON file path as a command-line argument, parses the record batches
 * from the JSON data, serializes them into Arrow IPC stream format, and writes the binary
 * stream to stdout. The output can be redirected to a file or piped to another program.
 *
 * Usage: file_to_stream <json_file_path>
 *
 * @param argc Number of command-line arguments
 * @param argv Array of command-line arguments
 * @return EXIT_SUCCESS on success, EXIT_FAILURE on error
 */
int main(int argc, char* argv[])
{
    // Check command-line arguments
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <json_file_path>\n";
        std::cerr << "Reads a JSON file and outputs the serialized Arrow IPC stream to stdout.\n";
        return EXIT_FAILURE;
    }

    const std::filesystem::path json_path(argv[1]);

    try
    {
        // Check if the JSON file exists
        if (!std::filesystem::exists(json_path))
        {
            std::cerr << "Error: File not found: " << json_path << "\n";
            return EXIT_FAILURE;
        }

        // Open and parse the JSON file
        std::ifstream json_file(json_path);
        if (!json_file.is_open())
        {
            std::cerr << "Error: Could not open file: " << json_path << "\n";
            return EXIT_FAILURE;
        }

        nlohmann::json json_data;
        try
        {
            json_data = nlohmann::json::parse(json_file);
        }
        catch (const nlohmann::json::parse_error& e)
        {
            std::cerr << "Error: Failed to parse JSON file: " << e.what() << "\n";
            return EXIT_FAILURE;
        }
        json_file.close();

        // Get the number of batches
        if (!json_data.contains("batches") || !json_data["batches"].is_array())
        {
            std::cerr << "Error: JSON file does not contain a 'batches' array.\n";
            return EXIT_FAILURE;
        }

        const size_t num_batches = json_data["batches"].size();

        // Parse all record batches from JSON
        std::vector<sparrow::record_batch> record_batches;
        record_batches.reserve(num_batches);

        for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
        {
            try
            {
                record_batches.emplace_back(
                    sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                );
            }
            catch (const std::exception& e)
            {
                std::cerr << "Error: Failed to build record batch " << batch_idx << ": " << e.what()
                          << "\n";
                return EXIT_FAILURE;
            }
        }

        // Serialize record batches to Arrow IPC stream format
        std::vector<uint8_t> stream_data;
        sparrow_ipc::memory_output_stream stream(stream_data);
        sparrow_ipc::serializer serializer(stream);

        serializer << record_batches << sparrow_ipc::end_stream;

        // Write the binary stream to stdout
        std::cout.write(reinterpret_cast<const char*>(stream_data.data()), stream_data.size());
        std::cout.flush();

        return EXIT_SUCCESS;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }
}
