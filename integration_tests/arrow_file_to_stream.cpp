#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

#include "integration_tools.hpp"

/**
 * @brief Reads an Arrow IPC file and outputs the serialized Arrow IPC stream to a file.
 *
 * This program takes an Arrow IPC file path and an output file path as command-line arguments,
 * deserializes the record batches from the Arrow file, serializes them into Arrow IPC stream format,
 * and writes the binary stream to the specified output file.
 *
 * Usage: arrow_file_to_stream <arrow_file_path> <output_stream_file>
 *
 * @param argc Number of command-line arguments
 * @param argv Array of command-line arguments
 * @return EXIT_SUCCESS on success, EXIT_FAILURE on error
 */
int main(int argc, char* argv[])
{
    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " <arrow_file_path> <output_stream_file>\n";
        std::cerr << "Reads an Arrow IPC file and outputs the serialized Arrow IPC stream to a file.\n";
        return EXIT_FAILURE;
    }

    const std::filesystem::path input_path(argv[1]);
    const std::filesystem::path output_path(argv[2]);

    try
    {
        if (!std::filesystem::exists(input_path))
        {
            std::cerr << "Error: Input file not found: " << input_path << "\n";
            return EXIT_FAILURE;
        }

        std::ifstream input_file(input_path, std::ios::binary);
        if (!input_file.is_open())
        {
            std::cerr << "Error: Could not open input file: " << input_path << "\n";
            return EXIT_FAILURE;
        }

        const std::vector<uint8_t> file_data(
            (std::istreambuf_iterator<char>(input_file)),
            std::istreambuf_iterator<char>()
        );
        input_file.close();

        if (file_data.empty())
        {
            std::cerr << "Error: Input file is empty.\n";
            return EXIT_FAILURE;
        }

        const std::vector<uint8_t> stream_data = integration_tools::file_to_stream(file_data);

        std::ofstream output_file(output_path, std::ios::binary);
        if (!output_file.is_open())
        {
            std::cerr << "Error: Could not open output file: " << output_path << "\n";
            return EXIT_FAILURE;
        }

        output_file.write(
            reinterpret_cast<const char*>(stream_data.data()),
            static_cast<std::streamsize>(stream_data.size())
        );
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
