#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <vector>

#include "integration_tools.hpp"

/**
 * @brief Reads a JSON file containing record batches and outputs the serialized Arrow IPC stream to stdout.
 *
 * This program takes a JSON file path as a command-line argument, parses the record batches
 * from the JSON data, serializes them into Arrow IPC stream format, and writes the binary
 * stream to stdout. The output can be redirected to a file or piped to another program.
 *
 * Usage: arrow_file_to_stream <json_file_path>
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
        // Convert JSON file to stream using the library
        std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_path);

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
