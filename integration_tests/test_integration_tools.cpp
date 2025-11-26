#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <sparrow/record_batch.hpp>
#include <sparrow/json_reader/json_parser.hpp>

#include "doctest/doctest.h"
#include "integration_tools.hpp"
#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/stream_file_serializer.hpp"

TEST_SUITE("Integration Tools Tests")
{
    // Get paths to test data
    const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;
    const std::filesystem::path tests_resources_files_path = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                             / "integration" / "cpp-21.0.0";

    TEST_CASE("json_file_to_stream - Non-existent file")
    {
        const std::filesystem::path non_existent = "non_existent_file_12345.json";
        CHECK_THROWS_AS(integration_tools::json_file_to_stream(non_existent), std::runtime_error);
    }

    TEST_CASE("stream_to_file - Empty input")
    {
        std::vector<uint8_t> empty_data;
        CHECK_THROWS_AS(integration_tools::stream_to_file(std::span<const uint8_t>(empty_data)), std::runtime_error);
    }

    TEST_CASE("validate_json_against_stream - Non-existent JSON file")
    {
        const std::filesystem::path non_existent = "non_existent_file_12345.json";
        std::vector<uint8_t> dummy_stream = {1, 2, 3};
        CHECK_THROWS_AS(
            integration_tools::validate_json_against_stream(non_existent, std::span<const uint8_t>(dummy_stream)),
            std::runtime_error
        );
    }

    TEST_CASE("json_file_to_arrow_file - Convert JSON to Arrow file format")
    {
        // Test with generated_primitive.json
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Convert JSON to Arrow file format
        std::vector<uint8_t> arrow_file_data;
        CHECK_NOTHROW(arrow_file_data = integration_tools::json_file_to_arrow_file(json_file));
        CHECK_GT(arrow_file_data.size(), 0);

        // Verify the output is a valid Arrow file by deserializing it
        auto batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(arrow_file_data));
        REQUIRE_EQ(batches.size(), 2);  // generated_primitive.json has 2 batches

        // Verify first batch has 17 rows (from JSON)
        CHECK_EQ(batches[0].nb_rows(), 17);
        // Verify second batch has 20 rows (from JSON)
        CHECK_EQ(batches[1].nb_rows(), 20);

        // Verify all 22 columns are present (from the JSON schema)
        CHECK_EQ(batches[0].nb_columns(), 22);
        CHECK_EQ(batches[1].nb_columns(), 22);

        // Validate the Arrow file content matches the original JSON
        std::ifstream json_input(json_file);
        REQUIRE(json_input.is_open());
        nlohmann::json json_data = nlohmann::json::parse(json_input);
        json_input.close();

        // Build record batches from JSON and compare with deserialized ones
        for (size_t i = 0; i < batches.size(); ++i)
        {
            auto expected_batch = sparrow::json_reader::build_record_batch_from_json(json_data, i);
            CHECK(integration_tools::compare_record_batch(expected_batch, batches[i], i, false));
        }
    }

    TEST_CASE("json_file_to_stream - Convert JSON to stream")
    {
        // Test with a known good JSON file
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Convert using the library
        std::vector<uint8_t> stream_data;
        CHECK_NOTHROW(stream_data = integration_tools::json_file_to_stream(json_file));
        CHECK_GT(stream_data.size(), 0);

        // Verify the output is a valid stream by deserializing it
        auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
        CHECK_GT(batches.size(), 0);
    }

    TEST_CASE("stream_to_file - Process stream")
    {
        const std::filesystem::path input_stream = tests_resources_files_path / "generated_primitive.stream";

        if (!std::filesystem::exists(input_stream))
        {
            MESSAGE("Skipping test: test file not found at " << input_stream);
            return;
        }

        // Read input stream
        std::ifstream stream_file(input_stream, std::ios::binary);
        REQUIRE(stream_file.is_open());

        std::vector<uint8_t> input_data(
            (std::istreambuf_iterator<char>(stream_file)),
            std::istreambuf_iterator<char>()
        );
        stream_file.close();

        // Convert using the library
        std::vector<uint8_t> output_data;
        CHECK_NOTHROW(output_data = integration_tools::stream_to_file(std::span<const uint8_t>(input_data)));
        CHECK_GT(output_data.size(), 0);

        // Verify the output is valid Arrow file format
        auto batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(output_data));
        CHECK_GT(batches.size(), 0);
    }

    TEST_CASE("Round-trip: JSON -> stream -> file")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Step 1: JSON -> stream
        std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_file);
        REQUIRE_GT(stream_data.size(), 0);

        // Step 2: stream -> file  
        std::vector<uint8_t> file_data = integration_tools::stream_to_file(std::span<const uint8_t>(stream_data));
        REQUIRE_GT(file_data.size(), 0);

        // Step 3: Compare the results - both should deserialize to same data
        auto stream_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
        auto file_batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        REQUIRE_EQ(stream_batches.size(), file_batches.size());
        for (size_t i = 0; i < stream_batches.size(); ++i)
        {
            CHECK(integration_tools::compare_record_batch(stream_batches[i], file_batches[i], i, false));
        }
    }

    TEST_CASE("validate_json_against_stream - Successful validation")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Convert JSON to stream
        std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_file);

        // Validate
        bool matches = integration_tools::validate_json_against_stream(
            json_file,
            std::span<const uint8_t>(stream_data)
        );
        CHECK(matches);
    }

    TEST_CASE("validate_json_against_stream - With reference stream file")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";
        const std::filesystem::path stream_file = tests_resources_files_path / "generated_primitive.stream";

        if (!std::filesystem::exists(json_file) || !std::filesystem::exists(stream_file))
        {
            MESSAGE("Skipping test: test file(s) not found");
            return;
        }

        // Read the stream file
        std::ifstream stream_input(stream_file, std::ios::binary);
        REQUIRE(stream_input.is_open());

        std::vector<uint8_t> stream_data(
            (std::istreambuf_iterator<char>(stream_input)),
            std::istreambuf_iterator<char>()
        );
        stream_input.close();

        // Validate
        bool matches = integration_tools::validate_json_against_stream(
            json_file,
            std::span<const uint8_t>(stream_data)
        );
        CHECK(matches);
    }

    TEST_CASE("json_to_stream and validate - Round-trip with validation")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_binary.json";

        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Convert JSON to stream
        std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_file);
        REQUIRE_GT(stream_data.size(), 0);

        // Validate that the stream matches the JSON
        bool matches = integration_tools::validate_json_against_stream(
            json_file,
            std::span<const uint8_t>(stream_data)
        );
        CHECK(matches);

        // Also verify by deserializing
        auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
        CHECK_GT(batches.size(), 0);
    }

    TEST_CASE("compare_record_batch - Identical batches")
    {
        // Create two identical batches
        auto int_array1 = sparrow::primitive_array<int32_t>({1, 2, 3});
        auto int_array2 = sparrow::primitive_array<int32_t>({1, 2, 3});

        sparrow::record_batch batch1({{"col", sparrow::array(std::move(int_array1))}});
        sparrow::record_batch batch2({{"col", sparrow::array(std::move(int_array2))}});

        CHECK(integration_tools::compare_record_batch(batch1, batch2, 0, false));
    }

    TEST_CASE("compare_record_batch - Different values")
    {
        // Create two batches with different values
        auto int_array1 = sparrow::primitive_array<int32_t>({1, 2, 3});
        auto int_array2 = sparrow::primitive_array<int32_t>({1, 2, 4});

        sparrow::record_batch batch1({{"col", sparrow::array(std::move(int_array1))}});
        sparrow::record_batch batch2({{"col", sparrow::array(std::move(int_array2))}});

        CHECK_FALSE(integration_tools::compare_record_batch(batch1, batch2, 0, false));
    }

    TEST_CASE("Multiple test files")
    {
        // Test with multiple files from the test data directory
        const std::vector<std::string> test_files = {
            "generated_primitive.json",
            "generated_binary.json",
            "generated_primitive_zerolength.json",
            "generated_binary_zerolength.json"
        };

        for (const auto& filename : test_files)
        {
            const std::filesystem::path json_file = tests_resources_files_path / filename;

            if (!std::filesystem::exists(json_file))
            {
                MESSAGE("Skipping test file: " << filename);
                continue;
            }

            SUBCASE(filename.c_str())
            {
                // Convert to stream
                std::vector<uint8_t> stream_data = integration_tools::json_file_to_stream(json_file);
                REQUIRE_GT(stream_data.size(), 0);

                // Validate
                bool matches = integration_tools::validate_json_against_stream(
                    json_file,
                    std::span<const uint8_t>(stream_data)
                );
                CHECK(matches);

                // Verify we can deserialize
                auto batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data));
                CHECK_GE(batches.size(), 0);
            }
        }
    }
}
