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


const std::filesystem::path tests_resources_files_path = TESTS_RESOURCES_FILES_PATH;

const std::vector<std::filesystem::path> files_paths_to_test = {
    tests_resources_files_path / "generated_primitive",
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

TEST_SUITE("integration tests")
{
    TEST_CASE("POUET")
    {
        for (const auto& file_path : files_paths_to_test)
        {
            std::filesystem::path json_path = file_path;
            json_path.replace_extension(".json");
            const std::string test_name = "Testing " + json_path.filename().string();
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
                const auto record_batches_from_stream = sparrow_ipc::deserialize_stream(stream_data.data());

                // Compare record batches
                REQUIRE_EQ(record_batches_from_stream.size(), record_batches_from_json.size());
                for (size_t i = 0; i < record_batches_from_stream.size(); ++i)
                {
                    for(size_t y = 0; y < record_batches_from_stream[i].nb_columns(); y++)
                    {
                        for(size_t z = 0 ; z < record_batches_from_stream[i].get_column(y).size(); z++)
                        {
                            INFO("Comparing batch " << i << ", column " << y << ", row " << z);
                            REQUIRE_EQ(record_batches_from_stream[i].get_column(y).size(), record_batches_from_json[i].get_column(y).size());
                            CHECK_EQ(record_batches_from_stream[i].get_column(y).at(z), record_batches_from_json[i].get_column(y).at(z));
                        }
                    }
                }
            }
        }
    }
}
