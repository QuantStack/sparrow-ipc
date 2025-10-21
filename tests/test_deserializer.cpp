#include <deque>
#include <list>
#include <numeric>
#include <vector>

#include <doctest/doctest.h>
#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/deserializer.hpp"
#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"
#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    // Helper function to serialize record batches to a byte buffer
    std::vector<uint8_t> serialize_record_batches(const std::vector<sp::record_batch>& batches)
    {
        std::vector<uint8_t> buffer;
        memory_output_stream stream(buffer);
        serializer ser(stream);
        ser << batches << end_stream;
        return buffer;
    }

    // Helper function to create multiple compatible record batches
    std::vector<sp::record_batch> create_test_record_batches(size_t count)
    {
        std::vector<sp::record_batch> batches;
        for (size_t i = 0; i < count; ++i)
        {
            auto int_array = sp::primitive_array<int32_t>({static_cast<int32_t>(i * 10),
                                                           static_cast<int32_t>(i * 10 + 1),
                                                           static_cast<int32_t>(i * 10 + 2)});
            auto string_array = sp::string_array(
                std::vector<std::string>{"batch_" + std::to_string(i) + "_a",
                                         "batch_" + std::to_string(i) + "_b",
                                         "batch_" + std::to_string(i) + "_c"}
            );
            batches.push_back(sp::record_batch({{"int_col", sp::array(std::move(int_array))},
                                                {"string_col", sp::array(std::move(string_array))}}));
        }
        return batches;
    }

    TEST_SUITE("deserializer")
    {
        TEST_CASE("construction with empty vector")
        {
            SUBCASE("Construct with empty vector reference")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);
                CHECK_EQ(batches.size(), 0);
            }
        }

        TEST_CASE("deserialize single record batch")
        {
            SUBCASE("Deserialize one batch into empty vector")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                // Create and serialize a single record batch
                auto original_batch = create_test_record_batch();
                auto serialized_data = serialize_record_batches({original_batch});

                // Deserialize
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                // Verify
                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_columns(), original_batch.nb_columns());
                CHECK_EQ(batches[0].nb_rows(), original_batch.nb_rows());
            }

            SUBCASE("Deserialize batch with different data types")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto int_array = sp::primitive_array<int32_t>({1, 2, 3});
                auto double_array = sp::primitive_array<double>({1.5, 2.5, 3.5});
                auto float_array = sp::primitive_array<float>({1.0f, 2.0f, 3.0f});

                auto rb = sp::record_batch({{"int_col", sp::array(std::move(int_array))},
                                           {"double_col", sp::array(std::move(double_array))},
                                           {"float_col", sp::array(std::move(float_array))}});

                auto serialized_data = serialize_record_batches({rb});
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_columns(), 3);
                CHECK_EQ(batches[0].nb_rows(), 3);
            }

            SUBCASE("Deserialize empty record batch")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto empty_batch = sp::record_batch({});
                auto serialized_data = serialize_record_batches({empty_batch});
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_columns(), 0);
            }
        }

        TEST_CASE("deserialize multiple record batches")
        {
            SUBCASE("Deserialize multiple batches at once")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                // Create multiple compatible batches
                auto original_batches = create_test_record_batches(3);
                auto serialized_data = serialize_record_batches(original_batches);

                // Deserialize
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                // Verify
                REQUIRE_EQ(batches.size(), 3);
                for (size_t i = 0; i < batches.size(); ++i)
                {
                    CHECK_EQ(batches[i].nb_columns(), original_batches[i].nb_columns());
                    CHECK_EQ(batches[i].nb_rows(), original_batches[i].nb_rows());
                }
            }

            SUBCASE("Deserialize large number of batches")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                const size_t num_batches = 100;
                auto original_batches = create_test_record_batches(num_batches);
                auto serialized_data = serialize_record_batches(original_batches);

                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), num_batches);
            }
        }

        TEST_CASE("incremental deserialization")
        {
            SUBCASE("Deserialize in multiple calls")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                // First deserialization
                auto batch1 = create_test_record_batches(2);
                auto serialized_data1 = serialize_record_batches(batch1);
                deser.deserialize(std::span<const uint8_t>(serialized_data1));

                CHECK_EQ(batches.size(), 2);

                // Second deserialization - should append to existing batches
                auto batch2 = create_test_record_batches(3);
                auto serialized_data2 = serialize_record_batches(batch2);
                deser.deserialize(std::span<const uint8_t>(serialized_data2));

                CHECK_EQ(batches.size(), 5);
            }

            SUBCASE("Multiple incremental deserializations")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                for (size_t i = 0; i < 5; ++i)
                {
                    auto new_batches = create_test_record_batches(2);
                    auto serialized_data = serialize_record_batches(new_batches);
                    deser.deserialize(std::span<const uint8_t>(serialized_data));

                    CHECK_EQ(batches.size(), (i + 1) * 2);
                }
            }

            SUBCASE("Deserialize into non-empty vector")
            {
                // Start with existing batches
                std::vector<sp::record_batch> batches = {create_test_record_batch()};
                CHECK_EQ(batches.size(), 1);

                deserializer deser(batches);

                // Add more batches
                auto new_batches = create_test_record_batches(2);
                auto serialized_data = serialize_record_batches(new_batches);
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                CHECK_EQ(batches.size(), 3);
            }
        }

        TEST_CASE("operator<< for deserialization")
        {
            SUBCASE("Single deserialization with <<")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto original_batches = create_test_record_batches(1);
                auto serialized_data = serialize_record_batches(original_batches);

                deser << std::span<const uint8_t>(serialized_data);

                REQUIRE_EQ(batches.size(), 1);
            }

            SUBCASE("Chain multiple deserializations with <<")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto batch1 = create_test_record_batches(1);
                auto serialized_data1 = serialize_record_batches(batch1);

                auto batch2 = create_test_record_batches(2);
                auto serialized_data2 = serialize_record_batches(batch2);

                auto batch3 = create_test_record_batches(1);
                auto serialized_data3 = serialize_record_batches(batch3);

                deser << std::span<const uint8_t>(serialized_data1)
                      << std::span<const uint8_t>(serialized_data2)
                      << std::span<const uint8_t>(serialized_data3);

                CHECK_EQ(batches.size(), 4);
            }

            SUBCASE("Mix deserialization methods")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto batch1 = create_test_record_batches(1);
                auto serialized_data1 = serialize_record_batches(batch1);
                deser.deserialize(std::span<const uint8_t>(serialized_data1));

                CHECK_EQ(batches.size(), 1);

                auto batch2 = create_test_record_batches(2);
                auto serialized_data2 = serialize_record_batches(batch2);
                deser << std::span<const uint8_t>(serialized_data2);

                CHECK_EQ(batches.size(), 3);
            }
        }

        TEST_CASE("deserialize with different container types")
        {
            SUBCASE("std::deque")
            {
                std::deque<sp::record_batch> batches;
                deserializer deser(batches);

                auto original_batches = create_test_record_batches(2);
                auto serialized_data = serialize_record_batches(original_batches);

                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 2);
            }

            SUBCASE("std::list")
            {
                std::list<sp::record_batch> batches;
                deserializer deser(batches);

                auto original_batches = create_test_record_batches(3);
                auto serialized_data = serialize_record_batches(original_batches);

                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 3);
            }
        }

        TEST_CASE("round-trip serialization and deserialization")
        {
            SUBCASE("Single batch round-trip")
            {
                auto original_batch = create_test_record_batch();
                auto serialized_data = serialize_record_batches({original_batch});

                std::vector<sp::record_batch> deserialized_batches;
                deserializer deser(deserialized_batches);
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(deserialized_batches.size(), 1);
                CHECK_EQ(deserialized_batches[0].nb_columns(), original_batch.nb_columns());
                CHECK_EQ(deserialized_batches[0].nb_rows(), original_batch.nb_rows());

                // Verify column names match
                for (size_t i = 0; i < original_batch.nb_columns(); ++i)
                {
                    CHECK_EQ(deserialized_batches[0].names()[i], original_batch.names()[i]);
                }
            }

            SUBCASE("Multiple batches round-trip")
            {
                auto original_batches = create_test_record_batches(5);
                auto serialized_data = serialize_record_batches(original_batches);

                std::vector<sp::record_batch> deserialized_batches;
                deserializer deser(deserialized_batches);
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(deserialized_batches.size(), original_batches.size());

                for (size_t i = 0; i < original_batches.size(); ++i)
                {
                    CHECK_EQ(deserialized_batches[i].nb_columns(), original_batches[i].nb_columns());
                    CHECK_EQ(deserialized_batches[i].nb_rows(), original_batches[i].nb_rows());
                }
            }

            SUBCASE("Double round-trip")
            {
                // First round-trip
                auto original_batches = create_test_record_batches(2);
                auto serialized_data1 = serialize_record_batches(original_batches);

                std::vector<sp::record_batch> deserialized_batches1;
                deserializer deser1(deserialized_batches1);
                deser1.deserialize(std::span<const uint8_t>(serialized_data1));

                // Second round-trip
                auto serialized_data2 = serialize_record_batches(deserialized_batches1);

                std::vector<sp::record_batch> deserialized_batches2;
                deserializer deser2(deserialized_batches2);
                deser2.deserialize(std::span<const uint8_t>(serialized_data2));

                // Verify both results match
                REQUIRE_EQ(deserialized_batches2.size(), original_batches.size());
                for (size_t i = 0; i < original_batches.size(); ++i)
                {
                    CHECK_EQ(deserialized_batches2[i].nb_columns(), original_batches[i].nb_columns());
                    CHECK_EQ(deserialized_batches2[i].nb_rows(), original_batches[i].nb_rows());
                }
            }
        }

        TEST_CASE("deserialize with complex data types")
        {
            SUBCASE("Mixed primitive types")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto int8_array = sp::primitive_array<int8_t>({1, 2, 3});
                auto int16_array = sp::primitive_array<int16_t>({100, 200, 300});
                auto int32_array = sp::primitive_array<int32_t>({1000, 2000, 3000});
                auto int64_array = sp::primitive_array<int64_t>({10000, 20000, 30000});

                auto rb = sp::record_batch({{"int8_col", sp::array(std::move(int8_array))},
                                           {"int16_col", sp::array(std::move(int16_array))},
                                           {"int32_col", sp::array(std::move(int32_array))},
                                           {"int64_col", sp::array(std::move(int64_array))}});

                auto serialized_data = serialize_record_batches({rb});
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_columns(), 4);
            }

            SUBCASE("String arrays")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto string_array = sp::string_array(
                    std::vector<std::string>{"hello", "world", "test", "data"}
                );
                auto rb = sp::record_batch({{"string_col", sp::array(std::move(string_array))}});

                auto serialized_data = serialize_record_batches({rb});
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_rows(), 4);
            }
        }

        TEST_CASE("edge cases")
        {
            SUBCASE("Deserialize empty data")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                // Create an empty batch
                auto empty_batch = sp::record_batch({});
                auto serialized_data = serialize_record_batches({empty_batch});

                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_columns(), 0);
            }

            SUBCASE("Very large batch")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                // Create a large array
                std::vector<int32_t> large_data(10000);
                std::iota(large_data.begin(), large_data.end(), 0);
                auto large_array = sp::primitive_array<int32_t>(large_data);
                auto rb = sp::record_batch({{"large_col", sp::array(std::move(large_array))}});

                auto serialized_data = serialize_record_batches({rb});
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_rows(), 10000);
            }

            SUBCASE("Single row batches")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto int_array = sp::primitive_array<int32_t>({42});
                auto string_array = sp::string_array(std::vector<std::string>{"single"});
                auto rb = sp::record_batch({{"int_col", sp::array(std::move(int_array))},
                                           {"string_col", sp::array(std::move(string_array))}});

                auto serialized_data = serialize_record_batches({rb});
                deser.deserialize(std::span<const uint8_t>(serialized_data));

                REQUIRE_EQ(batches.size(), 1);
                CHECK_EQ(batches[0].nb_rows(), 1);
            }
        }

        TEST_CASE("workflow example")
        {
            SUBCASE("Typical streaming deserialization workflow")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                // Simulate receiving data in chunks
                for (size_t i = 0; i < 3; ++i)
                {
                    auto chunk_batches = create_test_record_batches(2);
                    auto serialized_chunk = serialize_record_batches(chunk_batches);
                    deser << std::span<const uint8_t>(serialized_chunk);
                }

                // Verify all batches accumulated
                CHECK_EQ(batches.size(), 6);

                // Add one more batch using deserialize method
                auto final_batch = create_test_record_batches(1);
                auto serialized_final = serialize_record_batches(final_batch);
                deser.deserialize(std::span<const uint8_t>(serialized_final));

                CHECK_EQ(batches.size(), 7);
            }
        }

        TEST_CASE("deserializer with const container")
        {
            SUBCASE("Works with non-const reference")
            {
                std::vector<sp::record_batch> batches;
                deserializer deser(batches);

                auto original_batches = create_test_record_batches(1);
                auto serialized_data = serialize_record_batches(original_batches);

                deser.deserialize(std::span<const uint8_t>(serialized_data));

                CHECK_EQ(batches.size(), 1);
            }
        }
    }
}
