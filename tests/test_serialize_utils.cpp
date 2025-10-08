#include <chrono>

#include <doctest/doctest.h>
#include <sparrow.hpp>

#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/utils.hpp"
#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    TEST_SUITE("serialize_utils")
    {
        TEST_CASE("serialize_schema_message")
        {
            SUBCASE("Valid record batch")
            {
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                auto record_batch = create_test_record_batch();
                serialize_schema_message(record_batch, stream);

                CHECK_GT(serialized.size(), 0);

                // Check that it starts with continuation bytes
                CHECK_EQ(serialized.size() >= continuation.size(), true);
                for (size_t i = 0; i < continuation.size(); ++i)
                {
                    CHECK_EQ(serialized[i], continuation[i]);
                }

                // Check that the total size is aligned to 8 bytes
                CHECK_EQ(serialized.size() % 8, 0);
            }
        }

        TEST_CASE("fill_body")
        {
            SUBCASE("Simple primitive array")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto proxy = sp::detail::array_access::get_arrow_proxy(array);
                std::vector<uint8_t> body;
                sparrow_ipc::memory_output_stream stream(body);
                fill_body(proxy, stream);
                CHECK_GT(body.size(), 0);
                // Body size should be aligned
                CHECK_EQ(body.size() % 8, 0);
            }
        }

        TEST_CASE("generate_body")
        {
            SUBCASE("Record batch with multiple columns")
            {
                auto record_batch = create_test_record_batch();
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                generate_body(record_batch, stream);
                CHECK_GT(serialized.size(), 0);
                CHECK_EQ(serialized.size() % 8, 0);
            }
        }

        TEST_CASE("calculate_body_size")
        {
            SUBCASE("Single array")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto proxy = sp::detail::array_access::get_arrow_proxy(array);

                auto size = calculate_body_size(proxy);
                CHECK_GT(size, 0);
                CHECK_EQ(size % 8, 0);
            }

            SUBCASE("Record batch")
            {
                auto record_batch = create_test_record_batch();
                auto size = calculate_body_size(record_batch);
                CHECK_GT(size, 0);
                CHECK_EQ(size % 8, 0);
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                generate_body(record_batch, stream);
                CHECK_EQ(size, static_cast<int64_t>(serialized.size()));
            }
        }

        TEST_CASE("calculate_schema_message_size")
        {
            SUBCASE("Single column record batch")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto record_batch = sp::record_batch({{"column1", sp::array(std::move(array))}});

                const auto estimated_size = calculate_schema_message_size(record_batch);
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                // Verify by actual serialization
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                serialize_schema_message(record_batch, stream);

                CHECK_EQ(estimated_size, serialized.size());
            }

            SUBCASE("Multi-column record batch")
            {
                auto record_batch = create_test_record_batch();

                auto estimated_size = calculate_schema_message_size(record_batch);
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                serialize_schema_message(record_batch, stream);

                CHECK_EQ(estimated_size, serialized.size());
            }
        }

        TEST_CASE("calculate_record_batch_message_size")
        {
            SUBCASE("Single column record batch")
            {
                auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
                auto record_batch = sp::record_batch({{"column1", sp::array(std::move(array))}});

                auto estimated_size = calculate_record_batch_message_size(record_batch);
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                serialize_record_batch(record_batch, stream);

                CHECK_EQ(estimated_size, serialized.size());
            }

            SUBCASE("Multi-column record batch")
            {
                auto record_batch = create_test_record_batch();

                auto estimated_size = calculate_record_batch_message_size(record_batch);
                CHECK_GT(estimated_size, 0);
                CHECK_EQ(estimated_size % 8, 0);

                // Verify by actual serialization
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                serialize_record_batch(record_batch, stream);

                CHECK_EQ(estimated_size, serialized.size());
            }
        }

        TEST_CASE("calculate_total_serialized_size")
        {
            SUBCASE("Single record batch")
            {
                auto record_batch = create_test_record_batch();
                std::vector<sp::record_batch> batches = {record_batch};

                auto estimated_size = calculate_total_serialized_size(batches);
                CHECK_GT(estimated_size, 0);

                // Should equal schema size + record batch size
                auto schema_size = calculate_schema_message_size(record_batch);
                auto batch_size = calculate_record_batch_message_size(record_batch);
                CHECK_EQ(estimated_size, schema_size + batch_size);
            }

            SUBCASE("Multiple record batches")
            {
                auto array1 = sp::primitive_array<int32_t>({1, 2, 3});
                auto array2 = sp::primitive_array<double>({1.0, 2.0, 3.0});
                auto record_batch1 = sp::record_batch(
                    {{"col1", sp::array(std::move(array1))}, {"col2", sp::array(std::move(array2))}}
                );

                auto array3 = sp::primitive_array<int32_t>({4, 5, 6});
                auto array4 = sp::primitive_array<double>({4.0, 5.0, 6.0});
                auto record_batch2 = sp::record_batch(
                    {{"col1", sp::array(std::move(array3))}, {"col2", sp::array(std::move(array4))}}
                );

                std::vector<sp::record_batch> batches = {record_batch1, record_batch2};

                auto estimated_size = calculate_total_serialized_size(batches);
                CHECK_GT(estimated_size, 0);

                // Should equal schema size + sum of record batch sizes
                auto schema_size = calculate_schema_message_size(batches[0]);
                auto batch1_size = calculate_record_batch_message_size(batches[0]);
                auto batch2_size = calculate_record_batch_message_size(batches[1]);
                CHECK_EQ(estimated_size, schema_size + batch1_size + batch2_size);
            }

            SUBCASE("Empty collection")
            {
                std::vector<sp::record_batch> empty_batches;
                auto estimated_size = calculate_total_serialized_size(empty_batches);
                CHECK_EQ(estimated_size, 0);
            }

            SUBCASE("Inconsistent schemas throw exception")
            {
                auto array1 = sp::primitive_array<int32_t>({1, 2, 3});
                auto record_batch1 = sp::record_batch({{"col1", sp::array(std::move(array1))}});

                auto array2 = sp::primitive_array<double>({1.0, 2.0, 3.0});
                auto record_batch2 = sp::record_batch(
                    {{"col2", sp::array(std::move(array2))}}  // Different column name
                );

                std::vector<sp::record_batch> batches = {record_batch1, record_batch2};

                CHECK_THROWS_AS(auto size = calculate_total_serialized_size(batches), std::invalid_argument);
            }
        }

        TEST_CASE("memory_reservation_performance")
        {
            SUBCASE("Large record batch benefits from size estimation")
            {
                // Create a larger record batch for testing memory reservation
                std::vector<int32_t> large_data;
                large_data.reserve(10000);
                for (int i = 0; i < 10000; ++i)
                {
                    large_data.push_back(i);
                }

                auto array = sp::primitive_array<int32_t>(large_data);
                auto record_batch = sp::record_batch({{"large_column", sp::array(std::move(array))}});

                // Test with size estimation (current implementation)
                std::vector<uint8_t> with_estimation;
                memory_output_stream stream_with_estimation(with_estimation);

                // Reserve memory based on calculated size to avoid reallocations
                const std::size_t total_size = calculate_record_batch_message_size(record_batch);
                stream_with_estimation.reserve(stream_with_estimation.size() + total_size);

                auto start_time = std::chrono::high_resolution_clock::now();
                serialize_record_batch(record_batch, stream_with_estimation);
                auto end_time = std::chrono::high_resolution_clock::now();
                auto duration_with_estimation = end_time - start_time;

                // Verify size estimation accuracy
                auto estimated_size = calculate_record_batch_message_size(record_batch);
                CHECK_EQ(estimated_size, with_estimation.size());

                // The serialization should complete successfully
                CHECK_GT(with_estimation.size(), 0);
                CHECK_EQ(with_estimation.size() % 8, 0);

                // Test without size estimation (no pre-reservation)
                std::vector<uint8_t> without_estimation;
                memory_output_stream stream_without_estimation(without_estimation);
                auto start_time_no_est = std::chrono::high_resolution_clock::now();
                serialize_record_batch(record_batch, stream_without_estimation);
                auto end_time_no_est = std::chrono::high_resolution_clock::now();
                auto duration_without_estimation = end_time_no_est - start_time_no_est;
                DOCTEST_MESSAGE(
                    "With estimation: "
                    << std::chrono::duration_cast<std::chrono::microseconds>(duration_with_estimation).count()
                    << " us, Without estimation: "
                    << std::chrono::duration_cast<std::chrono::microseconds>(duration_without_estimation).count()
                    << " us"
                );
            }
        }

        TEST_CASE("serialize_record_batch")
        {
            SUBCASE("Valid record batch")
            {
                auto record_batch = create_test_record_batch();
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                serialize_record_batch(record_batch, stream);
                CHECK_GT(serialized.size(), 0);

                // Check that it starts with continuation bytes
                CHECK_GE(serialized.size(), continuation.size());
                for (size_t i = 0; i < continuation.size(); ++i)
                {
                    CHECK_EQ(serialized[i], continuation[i]);
                }

                // Check that the metadata part is aligned to 8 bytes
                // Find the end of metadata (before body starts)
                size_t continuation_size = continuation.size();
                size_t length_prefix_size = sizeof(uint32_t);

                CHECK_GT(serialized.size(), continuation_size + length_prefix_size);

                // Extract message length
                uint32_t message_length;
                std::memcpy(&message_length, serialized.data() + continuation_size, sizeof(uint32_t));

                size_t metadata_end = continuation_size + length_prefix_size + message_length;
                size_t aligned_metadata_end = utils::align_to_8(static_cast<int64_t>(metadata_end));

                // Verify alignment
                CHECK_EQ(aligned_metadata_end % 8, 0);
                CHECK_LE(aligned_metadata_end, serialized.size());
            }

            SUBCASE("Empty record batch")
            {
                auto empty_batch = sp::record_batch({});
                std::vector<uint8_t> serialized;
                memory_output_stream stream(serialized);
                serialize_record_batch(empty_batch, stream);
                CHECK_GT(serialized.size(), 0);
                CHECK_GE(serialized.size(), continuation.size());
            }
        }
    }
}