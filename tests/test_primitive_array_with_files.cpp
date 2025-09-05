#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

#include <nlohmann/json.hpp>

#include <sparrow/record_batch.hpp>

#include "sparrow/json_reader/json_parser.hpp"

#include "doctest/doctest.h"
#include "sparrow.hpp"
#include "sparrow_ipc/deserialize.hpp"


const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;

const std::filesystem::path tests_resources_files_path = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                         / "integration" / "1.0.0-littleendian";

const std::vector<std::filesystem::path> files_paths_to_test = {
    tests_resources_files_path / "generated_primitive",
    // tests_resources_files_path / "generated_primitive_large_offsets",
    tests_resources_files_path / "generated_primitive_zerolength",
    tests_resources_files_path / "generated_primitive_no_batches"
};

size_t get_number_of_batches(const std::filesystem::path& json_path)
{
    std::ifstream json_file(json_path);
    if (!json_file.is_open())
    {
        throw std::runtime_error("Could not open file: " + json_path.string());
    }
    const nlohmann::json data = nlohmann::json::parse(json_file);
    return data["batches"].size();
}

nlohmann::json load_json_file(const std::filesystem::path& json_path)
{
    std::ifstream json_file(json_path);
    if (!json_file.is_open())
    {
        throw std::runtime_error("Could not open file: " + json_path.string());
    }
    return nlohmann::json::parse(json_file);
}

TEST_SUITE("Integration tests")
{
    TEST_CASE("Compare stream deserialization with JSON deserialization")
    {
        for (const auto& file_path : files_paths_to_test)
        {
            std::filesystem::path json_path = file_path;
            json_path.replace_extension(".json");
            const std::string test_name = "Testing " + file_path.filename().string();
            SUBCASE(test_name.c_str())
            {
                // Load the JSON file
                auto json_data = load_json_file(json_path);
                CHECK(json_data != nullptr);

                const size_t num_batches = get_number_of_batches(json_path);

                std::vector<sparrow::record_batch> record_batches_from_json;

                for (size_t batch_idx = 0; batch_idx < num_batches; ++batch_idx)
                {
                    INFO("Processing batch " << batch_idx << " of " << num_batches);
                    record_batches_from_json.emplace_back(
                        sparrow::json_reader::build_record_batch_from_json(json_data, batch_idx)
                    );
                }

                // Load stream file
                std::filesystem::path stream_file_path = file_path;
                stream_file_path.replace_extension(".stream");
                std::ifstream stream_file(stream_file_path, std::ios::in | std::ios::binary);
                REQUIRE(stream_file.is_open());
                const std::vector<uint8_t> stream_data(
                    (std::istreambuf_iterator<char>(stream_file)),
                    (std::istreambuf_iterator<char>())
                );
                stream_file.close();

                // Process the stream file
                const auto record_batches_from_stream = sparrow_ipc::deserialize_stream(
                    std::span<const uint8_t>(stream_data)
                );

                // Compare record batches
                REQUIRE_EQ(record_batches_from_stream.size(), record_batches_from_json.size());
                for (size_t i = 0; i < record_batches_from_stream.size(); ++i)
                {
                    for (size_t y = 0; y < record_batches_from_stream[i].nb_columns(); y++)
                    {
                        const auto& column_stream = record_batches_from_stream[i].get_column(y);
                        const auto& column_json = record_batches_from_json[i].get_column(y);
                        REQUIRE_EQ(column_stream.size(), column_json.size());
                        for (size_t z = 0; z < column_json.size(); z++)
                        {
                            const auto col_name = column_stream.name().value_or("NA");
                            INFO(
                                "Comparing batch " << i << ", column " << y << " named :" << col_name
                                                   << " , row " << z
                            );
                            const auto& column_stream_value = column_stream[z];
                            const auto& column_json_value = column_json[z];
                            CHECK_EQ(column_stream_value, column_json_value);
                        }
                    }
                }
            }
        }
    }
}
