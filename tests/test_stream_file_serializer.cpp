#include <doctest/doctest.h>

#include <sparrow/array.hpp>
#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/stream_file_serializer.hpp"

TEST_SUITE("Stream file serializer tests")
{
    TEST_CASE("Basic file serialization with stream_file_serializer")
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

        sparrow::record_batch batch(names, std::move(arrays));

        // Serialize using stream_file_serializer
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch << sparrow_ipc::end_file;
        }

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

        // Deserialize and verify
        auto deserialized_batches = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        REQUIRE_EQ(deserialized_batches.size(), 1);
        const auto& deserialized_batch = deserialized_batches[0];
        CHECK_EQ(deserialized_batch.nb_columns(), 2);
        CHECK_EQ(deserialized_batch.nb_rows(), 5);
        CHECK_EQ(deserialized_batch.names()[0], "int_col");
        CHECK_EQ(deserialized_batch.names()[1], "float_col");
    }

    TEST_CASE("File serialization with multiple batches using write()")
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

        // Serialize using stream_file_serializer
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer.write(batches);
            serializer.end();
        }

        // Deserialize and verify
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

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

    TEST_CASE("File serialization with operator<< chaining")
    {
        std::vector<std::string> names = {"data"};

        // Create first batch
        std::vector<int32_t> data1 = {1, 2, 3};
        sparrow::primitive_array<int32_t> array1(std::move(data1));
        std::vector<sparrow::array> arrays1;
        arrays1.emplace_back(std::move(array1));
        sparrow::record_batch batch1(names, std::move(arrays1));

        // Create second batch
        std::vector<int32_t> data2 = {4, 5, 6};
        sparrow::primitive_array<int32_t> array2(std::move(data2));
        std::vector<sparrow::array> arrays2;
        arrays2.emplace_back(std::move(array2));
        sparrow::record_batch batch2(names, std::move(arrays2));

        // Serialize using stream_file_serializer with chaining
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch1 << batch2 << sparrow_ipc::end_file;
        }

        // Deserialize and verify
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));

        REQUIRE_EQ(deserialized.size(), 2);
        CHECK_EQ(deserialized[0].nb_rows(), 3);
        CHECK_EQ(deserialized[1].nb_rows(), 3);
    }

    TEST_CASE("File serialization with compression")
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

        sparrow::record_batch batch(names, std::move(arrays));

        // Serialize with compression
        std::vector<uint8_t> compressed_data;
        sparrow_ipc::memory_output_stream mem_stream(compressed_data);
        
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream, sparrow_ipc::CompressionType::LZ4_FRAME);
            serializer << batch << sparrow_ipc::end_file;
        }

        // Deserialize and verify
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(compressed_data));

        REQUIRE_EQ(deserialized.size(), 1);
        const auto& deserialized_batch = deserialized[0];
        CHECK_EQ(deserialized_batch.nb_rows(), 100);

        const auto& col = deserialized_batch.get_column(0);
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

    TEST_CASE("File serialization with destructor auto-end")
    {
        std::vector<std::string> names = {"values"};
        
        std::vector<int32_t> data = {1, 2, 3, 4, 5};
        sparrow::primitive_array<int32_t> array(std::move(data));
        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(array));
        sparrow::record_batch batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        // Destructor should automatically call end()
        {
            sparrow_ipc::stream_file_serializer serializer(mem_stream);
            serializer << batch;
            // No explicit end() call - destructor will handle it
        }

        // Verify file is valid
        auto deserialized = sparrow_ipc::deserialize_file(std::span<const uint8_t>(file_data));
        REQUIRE_EQ(deserialized.size(), 1);
        CHECK_EQ(deserialized[0].nb_rows(), 5);
    }

    TEST_CASE("Error: explicit end without writing batches")
    {
        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        // Explicitly calling end() without writing batches should throw
        sparrow_ipc::stream_file_serializer serializer(mem_stream);
        CHECK_THROWS_AS(serializer.end(), std::runtime_error);
    }

    TEST_CASE("Error: write after end")
    {
        std::vector<std::string> names = {"data"};
        std::vector<int32_t> data = {1, 2, 3};
        sparrow::primitive_array<int32_t> array(std::move(data));
        std::vector<sparrow::array> arrays;
        arrays.emplace_back(std::move(array));
        sparrow::record_batch batch(names, std::move(arrays));

        std::vector<uint8_t> file_data;
        sparrow_ipc::memory_output_stream mem_stream(file_data);
        
        sparrow_ipc::stream_file_serializer serializer(mem_stream);
        serializer << batch;
        serializer.end();
        
        CHECK_THROWS_AS(serializer.write(batch), std::runtime_error);
    }
}
