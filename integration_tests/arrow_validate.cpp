#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

#include "integration_tools.hpp"

/**
 * @brief Validates that a JSON file and an Arrow stream file contain identical data.
 *
 * This program reads a JSON file containing Arrow record batches and an Arrow IPC
 * stream file, converts both to vectors of record batches, and compares them
 * element-by-element to ensure they are identical.
 *
 * Usage: validate <json_file_path> <stream_file_path>
 *
 * @param argc Number of command-line arguments
 * @param argv Array of command-line arguments
 * @return EXIT_SUCCESS if the files match, EXIT_FAILURE on error or mismatch
 */
int main(int argc, char* argv[])
{
    // Check command-line arguments
    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <json_file_path> <stream_file_path>\n";
        std::cerr << "Validates that a JSON file and an Arrow stream file contain identical data.\n";
        return EXIT_FAILURE;
    }

    const std::filesystem::path json_path(argv[1]);
    const std::filesystem::path stream_path(argv[2]);

    try
    {
        // Check if the stream file exists
        if (!std::filesystem::exists(stream_path))
        {
            std::cerr << "Error: Stream file not found: " << stream_path << "\n";
            return EXIT_FAILURE;
        }

        std::cout << "Loading JSON file: " << json_path << "\n";
        std::cout << "Loading stream file: " << stream_path << "\n";

        // Read the stream file
        std::ifstream stream_file(stream_path, std::ios::in | std::ios::binary);
        if (!stream_file.is_open())
        {
            std::cerr << "Error: Could not open stream file: " << stream_path << "\n";
            return EXIT_FAILURE;
        }

        std::vector<uint8_t> stream_data(
            (std::istreambuf_iterator<char>(stream_file)),
            std::istreambuf_iterator<char>()
        );
        stream_file.close();

        if (stream_data.empty())
        {
            std::cerr << "Error: Stream file is empty.\n";
            return EXIT_FAILURE;
        }

        // Validate using the library
        bool matches = integration_tools::validate_json_against_stream(
            json_path,
            std::span<const uint8_t>(stream_data)
        );

        if (matches)
        {
            std::cout << "\n✓ Validation successful: JSON and stream files contain identical data!\n";
            return EXIT_SUCCESS;
        }
        else
        {
            std::cerr << "\n✗ Validation failed: JSON and stream files contain different data.\n";
            return EXIT_FAILURE;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }
}
