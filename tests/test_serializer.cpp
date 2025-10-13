#include <vector>

#include <doctest/doctest.h>
#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/memory_output_stream.hpp"
#include "sparrow_ipc/serializer.hpp"
#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    TEST_SUITE("serializer")
    {
        TEST_CASE("construction and write single record batch")
        {
            SUBCASE("Valid record batch")
            {
                auto rb = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);
                serializer ser(stream);
                ser.write(rb);

                // After writing first record batch, should have schema + record batch
                CHECK_GT(buffer.size(), 0);
                CHECK_GT(stream.size(), 0);
            }

            SUBCASE("Empty record batch")
            {
                auto empty_batch = sp::record_batch({});
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(empty_batch);

                CHECK_GT(buffer.size(), 0);
            }
        }

        TEST_CASE("construction and write range of record batches")
        {
            SUBCASE("Valid record batches")
            {
                auto array1 = sp::primitive_array<int32_t>({1, 2, 3});
                auto array2 = sp::primitive_array<double>({1.0, 2.0, 3.0});
                auto rb1 = sp::record_batch(
                    {{"col1", sp::array(std::move(array1))}, {"col2", sp::array(std::move(array2))}}
                );

                auto array3 = sp::primitive_array<int32_t>({4, 5, 6});
                auto array4 = sp::primitive_array<double>({4.0, 5.0, 6.0});
                auto rb2 = sp::record_batch(
                    {{"col1", sp::array(std::move(array3))}, {"col2", sp::array(std::move(array4))}}
                );

                std::vector<sp::record_batch> record_batches = {rb1, rb2};
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(record_batches);

                // Should have schema + 2 record batches
                CHECK_GT(buffer.size(), 0);
                CHECK_GT(stream.size(), 0);
            }

            SUBCASE("Reserve is called correctly")
            {
                auto rb = create_test_record_batch();
                std::vector<sp::record_batch> record_batches = {rb};
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(record_batches);

                // Verify that buffer has been written
                CHECK_GT(buffer.size(), 0);
            }
        }

        TEST_CASE("write single record batch")
        {
            SUBCASE("Write after construction with single batch")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);
                size_t size_after_construction = buffer.size();

                // Create compatible record batch
                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({6, 7, 8}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"foo", "bar", "baz"}))}}
                );

                ser.write(rb2);

                CHECK_GT(buffer.size(), size_after_construction);
            }

            SUBCASE("Multiple writes")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);
                size_t initial_size = buffer.size();

                for (int i = 0; i < 3; ++i)
                {
                    auto rb = sp::record_batch(
                        {{"int_col", sp::array(sp::primitive_array<int32_t>({i}))},
                         {"string_col", sp::array(sp::string_array(std::vector<std::string>{"test"}))}}
                    );
                    ser.write(rb);
                }

                CHECK_GT(buffer.size(), initial_size);
            }

            SUBCASE("Mismatched schema throws exception")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);

                // Create record batch with different schema
                auto rb2 = sp::record_batch(
                    {{"different_col", sp::array(sp::primitive_array<int32_t>({1, 2, 3}))}}
                );

                CHECK_THROWS_AS(ser.write(rb2), std::invalid_argument);
            }
        }

        TEST_CASE("write range of record batches")
        {
            SUBCASE("Write range after construction")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);
                size_t initial_size = buffer.size();

                auto array1 = sp::primitive_array<int32_t>({10, 20});
                auto array2 = sp::string_array(std::vector<std::string>{"a", "b"});
                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(std::move(array1))},
                     {"string_col", sp::array(std::move(array2))}}
                );

                auto array3 = sp::primitive_array<int32_t>({30, 40});
                auto array4 = sp::string_array(std::vector<std::string>{"c", "d"});
                auto rb3 = sp::record_batch(
                    {{"int_col", sp::array(std::move(array3))},
                     {"string_col", sp::array(std::move(array4))}}
                );

                std::vector<sp::record_batch> new_batches = {rb2, rb3};
                ser.write(new_batches);

                CHECK_GT(buffer.size(), initial_size);
            }

            SUBCASE("Reserve is called during range write")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);

                auto rb2 = create_test_record_batch();
                auto rb3 = create_test_record_batch();
                std::vector<sp::record_batch> new_batches = {rb2, rb3};

                size_t size_before = buffer.size();
                ser.write(new_batches);

                // Reserve should have been called, buffer should have grown
                CHECK_GT(buffer.size(), size_before);
            }

            SUBCASE("Empty range write does nothing")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);
                size_t initial_size = buffer.size();

                std::vector<sp::record_batch> empty_batches;
                ser.write(empty_batches);

                CHECK_EQ(buffer.size(), initial_size);
            }

            SUBCASE("Mismatched schema in range throws exception")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);

                auto rb2 = create_test_record_batch();
                auto rb3 = sp::record_batch(
                    {{"different_col", sp::array(sp::primitive_array<int32_t>({1, 2, 3}))}}
                );

                std::vector<sp::record_batch> new_batches = {rb2, rb3};
                CHECK_THROWS_AS(ser.write(new_batches), std::invalid_argument);
            }
        }

        TEST_CASE("end serialization")
        {
            SUBCASE("End after construction")
            {
                auto rb = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb);
                size_t initial_size = buffer.size();

                ser.end();

                // End should add end-of-stream marker
                CHECK_GT(buffer.size(), initial_size);
            }

            SUBCASE("Cannot write after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);
                ser.end();

                auto rb2 = create_test_record_batch();
                CHECK_THROWS_AS(ser.write(rb2), std::runtime_error);
            }

            SUBCASE("Cannot write range after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb1);
                ser.end();

                std::vector<sp::record_batch> new_batches = {create_test_record_batch()};
                CHECK_THROWS_AS(ser.write(new_batches), std::runtime_error);
            }
        }

        TEST_CASE("stream size tracking")
        {
            SUBCASE("Size increases with each operation")
            {
                auto rb = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                size_t size_before = stream.size();
                serializer ser(stream);
                ser.write(rb);
                size_t size_after_construction = stream.size();

                CHECK_GT(size_after_construction, size_before);

                ser.write(rb);
                size_t size_after_write = stream.size();

                CHECK_GT(size_after_write, size_after_construction);
            }
        }

        TEST_CASE("large number of record batches")
        {
            SUBCASE("Handle many record batches efficiently")
            {
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                std::vector<sp::record_batch> batches;
                const int num_batches = 100;

                for (int i = 0; i < num_batches; ++i)
                {
                    auto array = sp::primitive_array<int32_t>({i, i+1, i+2});
                    batches.push_back(sp::record_batch({{"col", sp::array(std::move(array))}}));
                }

                serializer ser(stream);
                ser.write(batches);

                // Should have schema + all batches
                CHECK_GT(buffer.size(), 0);
                CHECK_GT(stream.size(), 0);
            }
        }

        TEST_CASE("different column types")
        {
            SUBCASE("Multiple primitive types")
            {
                auto int_array = sp::primitive_array<int32_t>({1, 2, 3});
                auto double_array = sp::primitive_array<double>({1.5, 2.5, 3.5});
                auto float_array = sp::primitive_array<float>({1.0f, 2.0f, 3.0f});

                auto rb = sp::record_batch(
                    {{"int_col", sp::array(std::move(int_array))},
                     {"double_col", sp::array(std::move(double_array))},
                     {"float_col", sp::array(std::move(float_array))}}
                );

                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser.write(rb);

                CHECK_GT(buffer.size(), 0);
            }
        }

        TEST_CASE("operator<< with single record batch")
        {
            SUBCASE("Single batch write using <<")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;
                size_t size_after_construction = buffer.size();

                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({6, 7, 8}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"foo", "bar", "baz"}))}}
                );

                ser << rb2;

                CHECK_GT(buffer.size(), size_after_construction);
            }

            SUBCASE("Chaining multiple single batch writes")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;
                size_t initial_size = buffer.size();

                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({10, 20}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"a", "b"}))}}
                );

                auto rb3 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({30, 40}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"c", "d"}))}}
                );

                auto rb4 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({50, 60}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"e", "f"}))}}
                );

                ser << rb2 << rb3 << rb4;

                CHECK_GT(buffer.size(), initial_size);
            }

            SUBCASE("Cannot use << after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;
                ser.end();

                auto rb2 = create_test_record_batch();
                CHECK_THROWS_AS(ser << rb2, std::runtime_error);
            }

            SUBCASE("Mismatched schema with << throws exception")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;

                auto rb2 = sp::record_batch(
                    {{"different_col", sp::array(sp::primitive_array<int32_t>({1, 2, 3}))}}
                );

                CHECK_THROWS_AS(ser << rb2, std::invalid_argument);
            }
        }

        TEST_CASE("operator<< with range of record batches")
        {
            SUBCASE("Range write using <<")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;
                size_t initial_size = buffer.size();

                std::vector<sp::record_batch> batches;
                for (int i = 0; i < 3; ++i)
                {
                    auto rb = sp::record_batch(
                        {{"int_col", sp::array(sp::primitive_array<int32_t>({i * 10}))},
                         {"string_col", sp::array(sp::string_array(std::vector<std::string>{"test"}))}}
                    );
                    batches.push_back(rb);
                }

                ser << batches;

                CHECK_GT(buffer.size(), initial_size);
            }

            SUBCASE("Chaining range and single batch writes")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;
                size_t initial_size = buffer.size();

                std::vector<sp::record_batch> batches;
                for (int i = 0; i < 2; ++i)
                {
                    auto rb = sp::record_batch(
                        {{"int_col", sp::array(sp::primitive_array<int32_t>({i}))},
                         {"string_col", sp::array(sp::string_array(std::vector<std::string>{"x"}))}}
                    );
                    batches.push_back(rb);
                }

                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({99}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"final"}))}}
                );

                ser << batches << rb2;

                CHECK_GT(buffer.size(), initial_size);
            }

            SUBCASE("Mixed chaining with multiple ranges")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;
                size_t initial_size = buffer.size();

                std::vector<sp::record_batch> batches1;
                batches1.push_back(sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({10}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"a"}))}}
                ));

                std::vector<sp::record_batch> batches2;
                batches2.push_back(sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({20}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"b"}))}}
                ));

                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({30}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"c"}))}}
                );

                ser << batches1 << rb2 << batches2;

                CHECK_GT(buffer.size(), initial_size);
            }

            SUBCASE("Cannot use << with range after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;
                ser.end();

                std::vector<sp::record_batch> batches = {create_test_record_batch()};
                CHECK_THROWS_AS(ser << batches, std::runtime_error);
            }

            SUBCASE("Mismatched schema in range with << throws exception")
            {
                auto rb1 = create_test_record_batch();
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                serializer ser(stream);
                ser << rb1;

                auto rb2 = create_test_record_batch();
                auto rb3 = sp::record_batch(
                    {{"different_col", sp::array(sp::primitive_array<int32_t>({1, 2, 3}))}}
                );

                std::vector<sp::record_batch> batches = {rb2, rb3};
                CHECK_THROWS_AS(ser << batches, std::invalid_argument);
            }
        }

        TEST_CASE("workflow example with << operator")
        {
            SUBCASE("Typical usage pattern with streaming syntax")
            {
                // Create initial record batch
                auto rb1 = create_test_record_batch();

                // Setup stream
                std::vector<uint8_t> buffer;
                memory_output_stream stream(buffer);

                // Create serializer and write initial batch
                serializer ser(stream);
                ser << rb1;
                size_t size_after_init = buffer.size();
                CHECK_GT(size_after_init, 0);

                // Stream more batches
                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({10, 20}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"x", "y"}))}}
                );
                
                ser << rb2;
                size_t size_after_rb2 = buffer.size();
                CHECK_GT(size_after_rb2, size_after_init);

                // Stream range of batches
                std::vector<sp::record_batch> more_batches;
                for (int i = 0; i < 3; ++i)
                {
                    auto rb = sp::record_batch(
                        {{"int_col", sp::array(sp::primitive_array<int32_t>({i}))},
                         {"string_col", sp::array(sp::string_array(std::vector<std::string>{"test"}))}}
                    );
                    more_batches.push_back(rb);
                }
                
                ser << more_batches;
                size_t size_after_range = buffer.size();
                CHECK_GT(size_after_range, size_after_rb2);

                // Mix single and range in one chain
                auto rb3 = create_test_record_batch();
                std::vector<sp::record_batch> final_batches = {create_test_record_batch()};
                
                ser << rb3 << final_batches;
                size_t size_after_chain = buffer.size();
                CHECK_GT(size_after_chain, size_after_range);

                // End serialization
                ser.end();
                CHECK_GT(buffer.size(), size_after_chain);
            }
        }
    }
}
