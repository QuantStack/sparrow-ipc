#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>

#include <nlohmann/json.hpp>
#include <sparrow/record_batch.hpp>

#include "sparrow/json_reader/json_parser.hpp"

#include <sparrow_ipc/deserialize.hpp>

/**
 * @brief Helper function to compare two record batches for equality.
 *
 * Compares the structure and data of two record batches element-by-element.
 * Reports detailed error messages for any mismatches found.
 *
 * @param rb1 The first record batch to compare
 * @param rb2 The second record batch to compare
 * @param batch_idx The index of the batch being compared (for error reporting)
 * @return true if the batches are identical, false otherwise
 */
bool compare_record_batch(
    const sparrow::record_batch& rb1,
    const sparrow::record_batch& rb2,
    size_t batch_idx
)
{
    bool all_match = true;

    // Check number of columns
    if (rb1.nb_columns() != rb2.nb_columns())
    {
        std::cerr << "Error: Batch " << batch_idx << " has different number of columns: " << rb1.nb_columns()
                  << " vs " << rb2.nb_columns() << "\n";
        return false;
    }

    // Check number of rows
    if (rb1.nb_rows() != rb2.nb_rows())
    {
        std::cerr << "Error: Batch " << batch_idx << " has different number of rows: " << rb1.nb_rows()
                  << " vs " << rb2.nb_rows() << "\n";
        return false;
    }

    // Check column names
    const auto& names1 = rb1.names();
    const auto& names2 = rb2.names();
    if (names1.size() != names2.size())
    {
        std::cerr << "Error: Batch " << batch_idx << " has different number of column names\n";
        all_match = false;
    }
    else
    {
        for (size_t i = 0; i < names1.size(); ++i)
        {
            if (names1[i] != names2[i])
            {
                std::cerr << "Error: Batch " << batch_idx << " column " << i << " has different name: '"
                          << names1[i] << "' vs '" << names2[i] << "'\n";
                all_match = false;
            }
        }
    }

    // Check each column
    for (size_t col_idx = 0; col_idx < rb1.nb_columns(); ++col_idx)
    {
        const auto& col1 = rb1.get_column(col_idx);
        const auto& col2 = rb2.get_column(col_idx);

        // Check column size
        if (col1.size() != col2.size())
        {
            std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx << " has different size: "
                      << col1.size() << " vs " << col2.size() << "\n";
            all_match = false;
            continue;
        }

        // Check column data type
        if (col1.data_type() != col2.data_type())
        {
            std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx
                      << " has different data type\n";
            all_match = false;
            continue;
        }

        // Check column name
        const auto col_name1 = col1.name();
        const auto col_name2 = col2.name();
        if (col_name1 != col_name2)
        {
            std::cerr << "Warning: Batch " << batch_idx << ", column " << col_idx
                      << " has different name in column metadata\n";
        }

        // Check each value in the column
        for (size_t row_idx = 0; row_idx < col1.size(); ++row_idx)
        {
            if (col1[row_idx] != col2[row_idx])
            {
                std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx << " ('"
                          << col_name1.value_or("unnamed") << "'), row " << row_idx
                          << " has different value\n";
                std::cerr << "  JSON value:   " << std::format("{}", col1[row_idx]) << "\n";
                std::cerr << "  Stream value: " << std::format("{}", col2[row_idx]) << "\n";
                all_match = false;
            }
        }
    }

    return all_match;
}

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
        // Check if the JSON file exists
        if (!std::filesystem::exists(json_path))
        {
            std::cerr << "Error: JSON file not found: " << json_path << "\n";
            return EXIT_FAILURE;
        }

        // Check if the stream file exists
        if (!std::filesystem::exists(stream_path))
        {
            std::cerr << "Error: Stream file not found: " << stream_path << "\n";
            return EXIT_FAILURE;
        }

        // Load and parse the JSON file
        std::cout << "Loading JSON file: " << json_path << "\n";
        std::ifstream json_file(json_path);
        if (!json_file.is_open())
        {
            std::cerr << "Error: Could not open JSON file: " << json_path << "\n";
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

        // Check for batches in JSON
        if (!json_data.contains("batches") || !json_data["batches"].is_array())
        {
            std::cerr << "Error: JSON file does not contain a 'batches' array.\n";
            return EXIT_FAILURE;
        }

        const size_t num_batches = json_data["batches"].size();
        std::cout << "JSON file contains " << num_batches << " batch(es)\n";

        // Parse all record batches from JSON
        std::vector<sparrow::record_batch> json_batches;
        json_batches.reserve(num_batches);

        for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
        {
            try
            {
                json_batches.emplace_back(
                    sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                );
            }
            catch (const std::exception& e)
            {
                std::cerr << "Error: Failed to build record batch " << batch_idx << " from JSON: "
                          << e.what() << "\n";
                return EXIT_FAILURE;
            }
        }

        // Load and deserialize the stream file
        std::cout << "Loading stream file: " << stream_path << "\n";
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

        // Deserialize the stream
        std::vector<sparrow::record_batch> stream_batches;
        try
        {
            stream_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error: Failed to deserialize stream: " << e.what() << "\n";
            return EXIT_FAILURE;
        }

        std::cout << "Stream file contains " << stream_batches.size() << " batch(es)\n";

        // Compare the number of batches
        if (json_batches.size() != stream_batches.size())
        {
            std::cerr << "Error: Number of batches mismatch!\n";
            std::cerr << "  JSON file:   " << json_batches.size() << " batch(es)\n";
            std::cerr << "  Stream file: " << stream_batches.size() << " batch(es)\n";
            return EXIT_FAILURE;
        }

        // Compare each batch
        std::cout << "Comparing " << json_batches.size() << " batch(es)...\n";
        bool all_match = true;
        for (size_t batch_idx = 0; batch_idx < json_batches.size(); ++batch_idx)
        {
            std::cout << "  Comparing batch " << batch_idx << "...\n";
            if (!compare_record_batch(json_batches[batch_idx], stream_batches[batch_idx], batch_idx))
            {
                all_match = false;
            }
        }

        if (all_match)
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
