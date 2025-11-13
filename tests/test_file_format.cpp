#include <cstdint>
#include <filesystem>
#include <fstream>
#include <vector>

#include <sparrow/record_batch.hpp>

#include "doctest/doctest.h"
#include "sparrow_ipc/deserialize.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/stream_file_format.hpp"

TEST_SUITE("File format tests")
{
    TEST_CASE("Round-trip: serialize and deserialize file format")
    {
        // Create a simple record batch
        std::vector<std::string> names = {"int_col", "float_col"};

        // Create int32 array: [1, 2, 3, 4, 5]
        std::vector<int32_t> int_data = {1, 2, 3, 4, 5};
        sparrow::primitive_array<int32_t> int_array(std::move(int_data));

        // Create float array: [1.1, 2.2, 3.3, 4.4, 5.5]
        std::vector<float> float_data = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};
        sparrow::primitive_array<float> float_array(std::move(float_data));

        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(int_array));
        arrays.emplace_back(std::move(float_array));

        std::vector<sparrow::record_batch> batches;
        batches.emplace_back(names, std::move(arrays));

        // Serialize to file format
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        sparrow_ipc::any_output_stream stream(mem_stream);
        sparrow_ipc::serialize_to_file(batches, stream, std::nullopt);

        // Check file has correct magic bytes
        REQUIRE(file_data.size() >= 18);  // Minimum file size
        CHECK_EQ(file_data[0], 'A');
        CHECK_EQ(file_data[1], 'R');
        CHECK_EQ(file_data[2], 'R');
        CHECK_EQ(file_data[3], 'O');
        CHECK_EQ(file_data[4], 'W');
        CHECK_EQ(file_data[5], '1');

        // Check trailing magic
        const size_t trailing_offset = file_data.size() - 6;
        CHECK_EQ(file_data[trailing_offset], 'A');
        CHECK_EQ(file_data[trailing_offset + 1], 'R');
        CHECK_EQ(file_data[trailing_offset + 2], 'R');
        CHECK_EQ(file_data[trailing_offset + 3], 'O');
        CHECK_EQ(file_data[trailing_offset + 4], 'W');
        CHECK_EQ(file_data[trailing_offset + 5], '1');

        // Deserialize
        auto deserialized_batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        // Verify
        REQUIRE_EQ(deserialized_batches.size(), 1);
        const auto& batch = deserialized_batches[0];
        CHECK_EQ(batch.nb_columns(), 2);
        CHECK_EQ(batch.nb_rows(), 5);
        CHECK_EQ(batch.names()[0], "int_col");
        CHECK_EQ(batch.names()[1], "float_col");
    }

    TEST_CASE("File format with multiple record batches")
    {
        std::vector<std::string> names = {"values"};
        std::vector<sparrow::record_batch> batches;

        // Create 3 batches
        for (int batch_idx = 0; batch_idx < 3; ++batch_idx)
        {
            std::vector<int32_t> data;
            for (int i = 0; i < 10; ++i)
            {
                data.push_back(batch_idx * 10 + i);
            }
            sparrow::primitive_array<int32_t> array(std::move(data));

            std::vector<sparrow::array> arrays;
            arrays.emplace_back(std::move(array));

            auto names_copy = names;
            batches.emplace_back(std::move(names_copy), std::move(arrays));
        }

        // Serialize
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        sparrow_ipc::any_output_stream stream(mem_stream);
        sparrow_ipc::serialize_to_file(batches, stream, std::nullopt);

        // Deserialize
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        // Verify
        REQUIRE_EQ(deserialized.size(), 3);

        for (size_t batch_idx = 0; batch_idx < 3; ++batch_idx)
        {
            const auto& batch = deserialized[batch_idx];
            CHECK_EQ(batch.nb_columns(), 1);
            CHECK_EQ(batch.nb_rows(), 10);

            const auto& col = batch.get_column(0);
            col.visit(
                [batch_idx](const auto& impl)
                {
                    if constexpr (sparrow::is_primitive_array_v<std::decay_t<decltype(impl)>>)
                    {
                        for (size_t i = 0; i < 10; ++i)
                        {
                            CHECK_EQ(impl[i].value(), static_cast<int32_t>(batch_idx * 10 + i));
                        }
                    }
                }
            );
        }
    }

    TEST_CASE("File format with compression")
    {
        std::vector<std::string> names = {"data"};

        std::vector<int32_t> data;
        for (int i = 0; i < 100; ++i)
        {
            data.push_back(i);
        }
        sparrow::primitive_array<int32_t> array(std::move(data));

        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(array));

        std::vector<sparrow::record_batch> batches;
        batches.emplace_back(names, std::move(arrays));

        // Serialize with compression
        std::vector<uint8_t> compressed_data;
        sparrow_ipc::memory_output_stream mem_stream(compressed_data);
        sparrow_ipc::any_output_stream stream(mem_stream);
        sparrow_ipc::serialize_to_file(batches, stream, sparrow_ipc::CompressionType::LZ4_FRAME);

        // Deserialize
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(compressed_data));

        // Verify
        REQUIRE_EQ(deserialized.size(), 1);
        const auto& batch = deserialized[0];
        CHECK_EQ(batch.nb_rows(), 100);

        const auto& col = batch.get_column(0);

        col.visit(
            [](const auto& impl)
            {
                if constexpr (sparrow::is_primitive_array_v<std::decay_t<decltype(impl)>>)
                {
                    for (size_t i = 0; i < 100; ++i)
                    {
                        CHECK_EQ(impl[i].value(), static_cast<int32_t>(i));
                    }
                }
            }
        );
    }

    TEST_CASE("Empty file format")
    {
        std::vector<sparrow::record_batch> empty_batches;

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        sparrow_ipc::any_output_stream stream(mem_stream);
        sparrow_ipc::serialize_to_file(empty_batches, stream, std::nullopt);

        // Should produce empty output
        CHECK(file_data.empty());
    }

    TEST_CASE("Invalid file format - wrong magic bytes")
    {
        std::vector<uint8_t> bad_data = {'W', 'R', 'O', 'N', 'G', '1', 0, 0};

        CHECK_THROWS_AS(sparrow_ipc::deserialize_file(std::span<const uint8_t>(bad_data)), std::runtime_error);
    }

    TEST_CASE("Invalid file format - too small")
    {
        std::vector<uint8_t> small_data = {'A', 'R', 'R', 'O', 'W', '1'};

        CHECK_THROWS_AS(sparrow_ipc::deserialize_file(std::span<const uint8_t>(small_data)), std::runtime_error);
    }
}
