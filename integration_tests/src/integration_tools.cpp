#include "integration_tools.hpp"

#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include "sparrow_ipc/stream_file_serializer.hpp"

#if defined(__cpp_lib_format)
#    include <format>
#endif

#include <sparrow_ipc/deserialize.hpp>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>

#include <sparrow/json_reader/json_parser.hpp>

namespace integration_tools
{
    std::vector<uint8_t> json_file_to_arrow_file(const std::filesystem::path& json_path)
    {
        // Convert JSON file to stream first
        std::vector<uint8_t> stream_data = json_file_to_stream(json_path);

        // Then convert stream to file format
        return stream_to_file(std::span<const uint8_t>(stream_data));
    }

    std::vector<uint8_t> json_file_to_stream(const std::filesystem::path& json_path)
    {
        // Check if the JSON file exists
        if (!std::filesystem::exists(json_path))
        {
            throw std::runtime_error("JSON file not found: " + json_path.string());
        }

        // Open and parse the JSON file
        std::ifstream json_file(json_path);
        if (!json_file.is_open())
        {
            throw std::runtime_error("Could not open JSON file: " + json_path.string());
        }

        nlohmann::json json_data;
        try
        {
            json_data = nlohmann::json::parse(json_file);
        }
        catch (const nlohmann::json::parse_error& e)
        {
            throw std::runtime_error("Failed to parse JSON file: " + std::string(e.what()));
        }
        json_file.close();

        return json_to_stream(json_data);
    }

    std::vector<uint8_t> json_to_stream(const nlohmann::json& json_data)
    {
        // Get the number of batches
        if (!json_data.contains("batches") || !json_data["batches"].is_array())
        {
            throw std::runtime_error("JSON file does not contain a 'batches' array");
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
                throw std::runtime_error(
                    "Failed to build record batch " + std::to_string(batch_idx) + ": " + e.what()
                );
            }
        }

        // Serialize record batches to Arrow IPC stream format
        std::vector<uint8_t> stream_data;
        sparrow_ipc::memory_output_stream stream(stream_data);
        sparrow_ipc::serializer serializer(stream);
        serializer << record_batches << sparrow_ipc::end_stream;

        return stream_data;
    }

    std::vector<uint8_t> stream_to_file(std::span<const uint8_t> input_stream_data)
    {
        if (input_stream_data.empty())
        {
            throw std::runtime_error("Input stream data is empty");
        }

        // Deserialize the stream to validate it and extract record batches
        std::vector<sparrow::record_batch> record_batches;
        try
        {
            record_batches = sparrow_ipc::deserialize_stream(input_stream_data);
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error("Failed to deserialize stream: " + std::string(e.what()));
        }

        // Re-serialize the record batches to ensure a valid output stream
        std::vector<uint8_t> output_stream_data;
        sparrow_ipc::memory_output_stream stream(output_stream_data);
        sparrow_ipc::stream_file_serializer serializer(stream);
        serializer << record_batches << sparrow_ipc::end_file;

        return output_stream_data;
    }

    bool compare_record_batch(
        const sparrow::record_batch& rb1,
        const sparrow::record_batch& rb2,
        size_t batch_idx,
        bool verbose
    )
    {
        bool all_match = true;

        // Check number of columns
        if (rb1.nb_columns() != rb2.nb_columns())
        {
            if (verbose)
            {
                std::cerr << "Error: Batch " << batch_idx << " has different number of columns: "
                          << rb1.nb_columns() << " vs " << rb2.nb_columns() << "\n";
            }
            return false;
        }

        // Check number of rows
        if (rb1.nb_rows() != rb2.nb_rows())
        {
            if (verbose)
            {
                std::cerr << "Error: Batch " << batch_idx << " has different number of rows: " << rb1.nb_rows()
                          << " vs " << rb2.nb_rows() << "\n";
            }
            return false;
        }

        // Check column names
        const auto& names1 = rb1.names();
        const auto& names2 = rb2.names();
        if (names1.size() != names2.size())
        {
            if (verbose)
            {
                std::cerr << "Error: Batch " << batch_idx << " has different number of column names\n";
            }
            all_match = false;
        }
        else
        {
            for (size_t i = 0; i < names1.size(); ++i)
            {
                if (names1[i] != names2[i])
                {
                    if (verbose)
                    {
                        std::cerr << "Error: Batch " << batch_idx << " column " << i
                                  << " has different name: '" << names1[i] << "' vs '" << names2[i] << "'\n";
                    }
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
                if (verbose)
                {
                    std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx
                              << " has different size: " << col1.size() << " vs " << col2.size() << "\n";
                }
                all_match = false;
                continue;
            }

            // Check column data type
            if (col1.data_type() != col2.data_type())
            {
                if (verbose)
                {
                    std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx
                              << " has different data type\n";
                }
                all_match = false;
                continue;
            }

            // Check column name
            const auto col_name1 = col1.name();
            const auto col_name2 = col2.name();
            if (col_name1 != col_name2)
            {
                if (verbose)
                {
                    std::cerr << "Warning: Batch " << batch_idx << ", column " << col_idx
                              << " has different name in column metadata\n";
                }
            }

            // Check each value in the column
            for (size_t row_idx = 0; row_idx < col1.size(); ++row_idx)
            {
                if (col1[row_idx] != col2[row_idx])
                {
                    if (verbose)
                    {
                        std::cerr << "Error: Batch " << batch_idx << ", column " << col_idx << " ('"
                                  << col_name1.value_or("unnamed") << "'), row " << row_idx
                                  << " has different value\n";
#if defined(__cpp_lib_format)
                        std::cerr << "  JSON value:   " << std::format("{}", col1[row_idx]) << "\n";
                        std::cerr << "  Stream value: " << std::format("{}", col2[row_idx]) << "\n";
#endif
                    }
                    all_match = false;
                }
            }
        }

        return all_match;
    }

    bool validate_json_against_stream(
        const std::filesystem::path& json_path,
        std::span<const uint8_t> stream_data
    )
    {
        // Check if the JSON file exists
        if (!std::filesystem::exists(json_path))
        {
            throw std::runtime_error("JSON file not found: " + json_path.string());
        }

        // Load and parse the JSON file
        std::ifstream json_file(json_path);
        if (!json_file.is_open())
        {
            throw std::runtime_error("Could not open JSON file: " + json_path.string());
        }

        nlohmann::json json_data;
        try
        {
            json_data = nlohmann::json::parse(json_file);
        }
        catch (const nlohmann::json::parse_error& e)
        {
            throw std::runtime_error("Failed to parse JSON file: " + std::string(e.what()));
        }
        json_file.close();

        // Check for batches in JSON
        if (!json_data.contains("batches") || !json_data["batches"].is_array())
        {
            throw std::runtime_error("JSON file does not contain a 'batches' array");
        }

        const size_t num_batches = json_data["batches"].size();

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
                throw std::runtime_error(
                    "Failed to build record batch " + std::to_string(batch_idx) + " from JSON: " + e.what()
                );
            }
        }

        // Deserialize the stream
        if (stream_data.empty())
        {
            throw std::runtime_error("Stream data is empty");
        }

        std::vector<sparrow::record_batch> stream_batches;
        try
        {
            stream_batches = sparrow_ipc::deserialize_file(stream_data);
        }
        catch (const std::exception& e)
        {
            throw std::runtime_error("Failed to deserialize stream: " + std::string(e.what()));
        }

        // Compare the number of batches
        if (json_batches.size() != stream_batches.size())
        {
            return false;
        }

        // Compare each batch
        for (size_t batch_idx = 0; batch_idx < json_batches.size(); ++batch_idx)
        {
            if (!compare_record_batch(json_batches[batch_idx], stream_batches[batch_idx], batch_idx, false))
            {
                return false;
            }
        }

        return true;
    }
}
