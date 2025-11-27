#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

#include "integration_tools.hpp"

/**
 * @brief Validates that a JSON file and an Arrow arrow file contain identical data.
 *
 * This program reads a JSON file containing Arrow record batches and an Arrow IPC
 * arrow file, converts both to vectors of record batches, and compares them
 * element-by-element to ensure they are identical.
 *
 * Usage: validate <json_file_path> <arrow_file_path>
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
        std::cerr << "Validates that a JSON file and an Arrow file contain identical data.\n";
        return EXIT_FAILURE;
    }

    const std::filesystem::path json_path(argv[1]);
    const std::filesystem::path arrow_file_path(argv[2]);

    try
    {
        // Check if the stream file exists
        if (!std::filesystem::exists(arrow_file_path))
        {
            std::cerr << "Error: Arrow file not found: " << arrow_file_path << "\n";
            return EXIT_FAILURE;
        }

        std::cout << "Loading JSON file: " << json_path << "\n";
        std::cout << "Loading Arrow file: " << arrow_file_path << "\n";

        // Read the stream file
        std::ifstream arrow_file(arrow_file_path, std::ios::in | std::ios::binary);
        if (!arrow_file.is_open())
        {
            std::cerr << "Error: Could not open arrow file: " << arrow_file_path << "\n";
            return EXIT_FAILURE;
        }

        std::vector<uint8_t> arrow_file_data(
            (std::istreambuf_iterator<char>(arrow_file)),
            std::istreambuf_iterator<char>()
        );
        arrow_file.close();

        if (arrow_file_data.empty())
        {
            std::cerr << "Error: Arrow file is empty.\n";
            return EXIT_FAILURE;
        }

        // Validate using the library
        bool matches = integration_tools::validate_json_against_arrow_file(
            json_path,
            std::span<const uint8_t>(arrow_file_data)
        );

        if (matches)
        {
            std::cout << "\n✓ Validation successful: JSON and Arrow files contain identical data!\n";
            return EXIT_SUCCESS;
        }
        else
        {
            std::cerr << "\n✗ Validation failed: JSON and Arrow files contain different data.\n";
            return EXIT_FAILURE;
        }
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }
}
