#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include <nlohmann/json.hpp>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/stream_file_serializer.hpp>

#include <sparrow/json_reader/json_parser.hpp>
#include <sparrow/record_batch.hpp>

/**
 * @brief Reads a JSON file containing record batches and writes the serialized Arrow IPC stream to a file.
 *
 * This program takes a JSON file path and an output file path as command-line arguments,
 * parses the record batches from the JSON data, serializes them into Arrow IPC stream format,
 * and writes the binary stream to the specified output file.
 *
 * Usage: json_to_file <json_file_path> <output_file_path>
 *
 * @param argc Number of command-line arguments
 * @param argv Array of command-line arguments
 * @return EXIT_SUCCESS on success, EXIT_FAILURE on error
 */
int main(int argc, char* argv[])
{
    // Check command-line arguments
    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <json_file_path> <output_file_path>\n";
        std::cerr << "Reads a JSON file and writes the serialized Arrow IPC stream to a file.\n";
        return EXIT_FAILURE;
    }

    const std::filesystem::path json_path(argv[1]);
    const std::filesystem::path output_path(argv[2]);

    try
    {
        // Check if the JSON file exists
        if (!std::filesystem::exists(json_path))
        {
            std::cerr << "Error: Input file not found: " << json_path << "\n";
            return EXIT_FAILURE;
        }

        // Open and parse the JSON file
        std::ifstream json_file(json_path);
        if (!json_file.is_open())
        {
            std::cerr << "Error: Could not open input file: " << json_path << "\n";
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
                std::cerr << "Error: Failed to build record batch " << batch_idx << ": " << e.what() << "\n";
                return EXIT_FAILURE;
            }
        }

        // Serialize record batches to Arrow IPC stream format
        std::vector<uint8_t> stream_data;
        sparrow_ipc::memory_output_stream stream(stream_data);
        sparrow_ipc::stream_file_serializer serializer(stream);
        serializer << record_batches;
        serializer.end();

        // Write the binary stream to the output file
        std::ofstream output_file(output_path, std::ios::out | std::ios::binary);
        if (!output_file.is_open())
        {
            std::cerr << "Error: Could not open output file: " << output_path << "\n";
            return EXIT_FAILURE;
        }

        output_file.write(reinterpret_cast<const char*>(stream_data.data()), stream_data.size());
        output_file.close();

        if (!output_file.good())
        {
            std::cerr << "Error: Failed to write to output file: " << output_path << "\n";
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }
}
