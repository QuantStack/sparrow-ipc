#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

#include <nlohmann/json.hpp>

#include <sparrow/record_batch.hpp>
#include <sparrow/utils/format.hpp>

#include "sparrow/json_reader/json_parser.hpp"

#include "doctest/doctest.h"
#include "sparrow.hpp"
#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"

const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;

const std::filesystem::path tests_resources_files_path = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                         / "integration" / "cpp-21.0.0";

const std::filesystem::path tests_resources_files_path_with_compression = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                         / "integration" / "2.0.0-compression";

const std::vector<std::filesystem::path> files_paths_to_test = {
    tests_resources_files_path / "generated_primitive",
    // tests_resources_files_path / "generated_primitive_large_offsets",
    tests_resources_files_path / "generated_primitive_zerolength",
    // tests_resources_files_path / "generated_primitive_no_batches"
};

const std::vector<std::filesystem::path> files_paths_to_test_with_compression = {
    tests_resources_files_path_with_compression / "generated_lz4",
    tests_resources_files_path_with_compression/ "generated_uncompressible_lz4"
//     tests_resources_files_path_with_compression / "generated_zstd"
//     tests_resources_files_path_with_compression/ "generated_uncompressible_zstd"
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

void compare_record_batches(
    const std::vector<sparrow::record_batch>& record_batches_1,
    const std::vector<sparrow::record_batch>& record_batches_2
)
{
    REQUIRE_EQ(record_batches_1.size(), record_batches_2.size());
//     std::cout << "record_batches1 size: " << record_batches_1.size() <<  " record_batches2 size: " << record_batches_2.size() << std::endl;
    for (size_t i = 0; i < record_batches_1.size(); ++i)
    {
        for (size_t y = 0; y < record_batches_1[i].nb_columns(); y++)
        {
//             std::cout << "record_batches1 nb cols: " << record_batches_1[i].nb_columns() <<  " record_batches2 nb cols: " << record_batches_2[i].nb_columns() << std::endl;
            const auto& column_1 = record_batches_1[i].get_column(y);
            const auto& column_2 = record_batches_2[i].get_column(y);
            REQUIRE_EQ(column_1.size(), column_2.size());
            CHECK_EQ(record_batches_1[i].names()[y], record_batches_2[i].names()[y]);
            for (size_t z = 0; z < column_1.size(); z++)
            {
                const auto col_name = column_1.name().value_or("NA");
                INFO("Comparing batch " << i << ", column " << y << " named :" << col_name << " , row " << z);
//                 std::cout << "Comparing batch " << i << ", column " << y << " named :" << col_name << " , row " << z << std::endl;
                REQUIRE_EQ(column_1.data_type(), column_2.data_type());
                const auto& column_1_value = column_1[z];
                const auto& column_2_value = column_2[z];
//                 std::cout << "column_1_value :" << column_1_value << " and " << column_2_value  << std::endl;
                CHECK_EQ(column_1_value, column_2_value);
            }
        }
    }
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
                compare_record_batches(record_batches_from_json, record_batches_from_stream);
            }
        }
    }

    TEST_CASE("Compare record_batch serialization with stream file")
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

                std::vector<uint8_t> serialized_data;
                sparrow_ipc::memory_output_stream stream(serialized_data);
                sparrow_ipc::serializer serializer(stream);
                serializer << record_batches_from_json << sparrow_ipc::end_stream;
                const auto deserialized_serialized_data = sparrow_ipc::deserialize_stream(
                    std::span<const uint8_t>(serialized_data)
                );
                compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
            }
        }
    }

    TEST_CASE("Compare record_batch serialization with stream file using LZ4 compression")
    {
        for (const auto& file_path : files_paths_to_test_with_compression)
        {
            std::filesystem::path json_path = file_path;
            json_path.replace_extension(".json");
            const std::string test_name = "Testing LZ4 compression with " + file_path.filename().string();
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
                const auto serialized_data = sparrow_ipc::serialize(record_batches_from_json, org::apache::arrow::flatbuf::CompressionType::LZ4_FRAME);
                const auto deserialized_serialized_data = sparrow_ipc::deserialize_stream(
                    std::span<const uint8_t>(serialized_data)
                );
                compare_record_batches(record_batches_from_stream, deserialized_serialized_data);
            }
        }
    }
}
