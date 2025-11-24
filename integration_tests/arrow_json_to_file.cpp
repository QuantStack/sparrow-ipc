#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include "integration_tools.hpp"

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
        // Convert JSON file to stream using the library
        std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_path);

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
