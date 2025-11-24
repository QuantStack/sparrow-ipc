#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

#include <sparrow_ipc/deserialize.hpp>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/stream_file_serializer.hpp>

/**
 * @brief Reads an Arrow IPC stream from a file and writes it to another file.
 *
 * This program reads a binary Arrow IPC stream from an input file, deserializes it
 * to verify its validity, then re-serializes it and writes the result to the specified
 * output file. This ensures the output file contains a valid Arrow IPC stream.
 *
 * Usage: stream_to_file <input_file_path> <output_file_path>
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
        std::cerr << "Usage: " << argv[0] << " <input_file_path> <output_file_path>\n";
        std::cerr << "Reads an Arrow IPC stream from a file and writes it to another file.\n";
        return EXIT_FAILURE;
    }

    const std::filesystem::path input_path(argv[1]);
    const std::filesystem::path output_path(argv[2]);

    try
    {
        // Check if the input file exists
        if (!std::filesystem::exists(input_path))
        {
            std::cerr << "Error: Input file not found: " << input_path << "\n";
            return EXIT_FAILURE;
        }

        // Read the entire stream from the input file
        std::ifstream input_file(input_path, std::ios::in | std::ios::binary);
        if (!input_file.is_open())
        {
            std::cerr << "Error: Could not open input file: " << input_path << "\n";
            return EXIT_FAILURE;
        }

        std::vector<uint8_t> input_stream_data(
            (std::istreambuf_iterator<char>(input_file)),
            std::istreambuf_iterator<char>()
        );
        input_file.close();

        if (input_stream_data.empty())
        {
            std::cerr << "Error: No data received from stdin.\n";
            return EXIT_FAILURE;
        }

        // Deserialize the stream to validate it and extract record batches
        std::vector<sparrow::record_batch> record_batches;
        try
        {
            record_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(input_stream_data));
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error: Failed to deserialize stream: " << e.what() << "\n";
            return EXIT_FAILURE;
        }

        // Re-serialize the record batches to ensure a valid output stream
        std::vector<uint8_t> output_stream_data;
        sparrow_ipc::memory_output_stream stream(output_stream_data);
        sparrow_ipc::stream_file_serializer serializer(stream);
        serializer << record_batches;
        serializer.end();

        // Write the stream to the output file
        std::ofstream output_file(output_path, std::ios::out | std::ios::binary);
        if (!output_file.is_open())
        {
            std::cerr << "Error: Could not open output file: " << output_path << "\n";
            return EXIT_FAILURE;
        }

        output_file.write(reinterpret_cast<const char*>(output_stream_data.data()), output_stream_data.size());
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
