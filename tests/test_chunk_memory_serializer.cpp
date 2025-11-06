#include <vector>

#include <doctest/doctest.h>
#include <sparrow/record_batch.hpp>

#include "sparrow_ipc/chunk_memory_output_stream.hpp"
#include "sparrow_ipc/chunk_memory_serializer.hpp"
#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    TEST_SUITE("chunk_serializer")
    {
        TEST_CASE("construction with single record batch")
        {
            SUBCASE("Valid record batch, with and without compression")
            {
                auto rb = create_compressible_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks_uncompressed;
                chunked_memory_output_stream stream_uncompressed(chunks_uncompressed);

                chunk_serializer serializer_uncompressed(stream_uncompressed);
                serializer_uncompressed << rb;

                CHECK_EQ(chunks_uncompressed.size(), 2);
                CHECK_GT(chunks_uncompressed[0].size(), 0);  // Schema message
                CHECK_GT(chunks_uncompressed[1].size(), 0);  // Record batch message
                CHECK_GT(stream_uncompressed.size(), 0);

                struct CompressionParams
                {
                    CompressionType type;
                    const char* name;
                };
                const auto params = {CompressionParams{CompressionType::LZ4_FRAME, "LZ4"},
                                     CompressionParams{CompressionType::ZSTD, "ZSTD"}};

                for (const auto& p : params)
                {
                    SUBCASE(p.name)
                    {
                        std::vector<std::vector<uint8_t>> chunks_compressed;
                        chunked_memory_output_stream stream_compressed(chunks_compressed);
                        chunk_serializer serializer_compressed(stream_compressed, p.type);
                        serializer_compressed << rb;

                        // After construction with single record batch, should have schema + record batch
                        CHECK_EQ(chunks_compressed.size(), 2);
                        CHECK_GT(chunks_compressed[0].size(), 0);  // Schema message
                        CHECK_GT(chunks_compressed[1].size(), 0);  // Record batch message
                        CHECK_GT(stream_compressed.size(), 0);

                        // Check that schema size is the same
                        CHECK_EQ(chunks_compressed[0].size(), chunks_uncompressed[0].size());

                        // Check that compressed record batch is smaller
                        CHECK_LT(chunks_compressed[1].size(), chunks_uncompressed[1].size());
                    }
                }
            }

            SUBCASE("Empty record batch")
            {
                auto empty_batch = sp::record_batch({});
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer << empty_batch;

                CHECK_EQ(chunks.size(), 2);
                CHECK_GT(chunks[0].size(), 0);
            }
        }

        TEST_CASE("construction with range of record batches")
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
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer << record_batches;

                // Should have schema + 2 record batches = 3 chunks
                CHECK_EQ(chunks.size(), 3);
                CHECK_GT(chunks[0].size(), 0);  // Schema
                CHECK_GT(chunks[1].size(), 0);  // First record batch
                CHECK_GT(chunks[2].size(), 0);  // Second record batch
            }


            SUBCASE("Reserve is called correctly")
            {
                auto rb = create_test_record_batch();
                std::vector<sp::record_batch> record_batches = {rb};
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer << record_batches;

                // Verify that chunks were reserved (capacity should be >= size)
                CHECK_GE(chunks.capacity(), chunks.size());
            }
        }

        TEST_CASE("write single record batch")
        {
            SUBCASE("Write after construction with single batch")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer << rb1;
                CHECK_EQ(chunks.size(), 2);  // Schema + rb1

                // Create compatible record batch
                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({6, 7, 8}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"foo", "bar", "baz"}))}}
                );

                serializer.write(rb2);

                CHECK_EQ(chunks.size(), 3);  // Schema + rb1 + rb2
                CHECK_GT(chunks[2].size(), 0);
            }

            SUBCASE("Multiple appends")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer << rb1;

                for (int i = 0; i < 3; ++i)
                {
                    auto rb = sp::record_batch(
                        {{"int_col", sp::array(sp::primitive_array<int32_t>({i}))},
                         {"string_col", sp::array(sp::string_array(std::vector<std::string>{"test"}))}}
                    );
                    serializer.write(rb);
                }

                CHECK_EQ(chunks.size(), 5);  // Schema + 1 initial + 3 appended
            }
        }

        TEST_CASE("write range of record batches")
        {
            SUBCASE("Write range after construction")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                CHECK_EQ(chunks.size(), 2);

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
                serializer.write(new_batches);

                CHECK_EQ(chunks.size(), 4);  // Schema + rb1 + rb2 + rb3
                CHECK_GT(chunks[2].size(), 0);
                CHECK_GT(chunks[3].size(), 0);
            }

            SUBCASE("Reserve is called during range append")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);

                auto rb2 = create_test_record_batch();
                auto rb3 = create_test_record_batch();
                std::vector<sp::record_batch> new_batches = {rb2, rb3};

                size_t old_capacity = chunks.capacity();
                serializer.write(new_batches);

                // Reserve should have been called
                CHECK_GE(chunks.capacity(), chunks.size());
            }

            SUBCASE("Empty range append does nothing")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                size_t initial_size = chunks.size();

                std::vector<sp::record_batch> empty_batches;
                serializer.write(empty_batches);

                CHECK_EQ(chunks.size(), initial_size);
            }
        }

        TEST_CASE("end serialization")
        {
            SUBCASE("End after construction")
            {
                auto rb = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb);
                size_t initial_size = chunks.size();

                serializer.end();

                // End should add end-of-stream marker
                CHECK_GT(chunks.size(), initial_size);
            }

            SUBCASE("Cannot append after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                serializer.end();

                auto rb2 = create_test_record_batch();
                CHECK_THROWS_AS(serializer.write(rb2), std::runtime_error);
            }

            SUBCASE("Cannot append range after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                serializer.end();

                std::vector<sp::record_batch> new_batches = {create_test_record_batch()};
                CHECK_THROWS_AS(serializer.write(new_batches), std::runtime_error);
            }
        }

        TEST_CASE("stream size tracking")
        {
            SUBCASE("Size increases with each operation")
            {
                auto rb = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                size_t size_before = stream.size();
                chunk_serializer serializer(stream);
                serializer.write(rb);
                size_t size_after_construction = stream.size();

                CHECK_GT(size_after_construction, size_before);

                serializer.write(rb);
                size_t size_after_append = stream.size();

                CHECK_GT(size_after_append, size_after_construction);
            }
        }

        TEST_CASE("large number of record batches")
        {
            SUBCASE("Handle many record batches efficiently")
            {
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                std::vector<sp::record_batch> batches;
                const int num_batches = 100;

                for (int i = 0; i < num_batches; ++i)
                {
                    auto array = sp::primitive_array<int32_t>({i, i+1, i+2});
                    batches.push_back(sp::record_batch({{"col", sp::array(std::move(array))}}));
                }

                chunk_serializer serializer(stream);
                serializer.write(batches);

                // Should have schema + all batches
                CHECK_EQ(chunks.size(), num_batches + 1);
                CHECK_GT(stream.size(), 0);

                // Verify each chunk has data
                for (const auto& chunk : chunks)
                {
                    CHECK_GT(chunk.size(), 0);
                }
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

                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb);

                CHECK_EQ(chunks.size(), 2);  // Schema + record batch
                CHECK_GT(chunks[0].size(), 0);
                CHECK_GT(chunks[1].size(), 0);
            }
        }

        TEST_CASE("workflow example")
        {
            SUBCASE("Typical usage pattern")
            {
                // Create initial record batch
                auto rb1 = create_test_record_batch();

                // Setup chunked stream
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                // Create serializer with initial batch
                chunk_serializer serializer(stream);
                    serializer.write(rb1);
                CHECK_EQ(chunks.size(), 2);

                // Append more batches
                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({10, 20}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"x", "y"}))}}
                );
                serializer.write(rb2);
                CHECK_EQ(chunks.size(), 3);

                // Append range of batches
                std::vector<sp::record_batch> more_batches;
                for (int i = 0; i < 3; ++i)
                {
                    auto rb = sp::record_batch(
                        {{"int_col", sp::array(sp::primitive_array<int32_t>({i}))},
                         {"string_col", sp::array(sp::string_array(std::vector<std::string>{"test"}))}}
                    );
                    more_batches.push_back(rb);
                }
                serializer.write(more_batches);
                CHECK_EQ(chunks.size(), 6);

                // End serialization
                serializer.end();
                CHECK_GT(chunks.size(), 6);

                // Verify all chunks have data
                for (const auto& chunk : chunks)
                {
                    CHECK_GT(chunk.size(), 0);
                }
            }
        }

        TEST_CASE("operator<< with single record batch")
        {
            SUBCASE("Single batch append using <<")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                CHECK_EQ(chunks.size(), 2);  // Schema + rb1

                auto rb2 = sp::record_batch(
                    {{"int_col", sp::array(sp::primitive_array<int32_t>({6, 7, 8}))},
                     {"string_col", sp::array(sp::string_array(std::vector<std::string>{"foo", "bar", "baz"}))}}
                );

                serializer << rb2;

                CHECK_EQ(chunks.size(), 3);  // Schema + rb1 + rb2
                CHECK_GT(chunks[2].size(), 0);
            }

            SUBCASE("Chaining multiple single batch appends")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);

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

                serializer << rb2 << rb3 << rb4;

                CHECK_EQ(chunks.size(), 5);  // Schema + 4 record batches
                CHECK_GT(chunks[2].size(), 0);
                CHECK_GT(chunks[3].size(), 0);
                CHECK_GT(chunks[4].size(), 0);
            }

            SUBCASE("Cannot use << after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                serializer.end();

                auto rb2 = create_test_record_batch();
                CHECK_THROWS_AS(serializer << rb2, std::runtime_error);
            }
        }

        TEST_CASE("operator<< with range of record batches")
        {
            SUBCASE("Range append using <<")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                CHECK_EQ(chunks.size(), 2);

                std::vector<sp::record_batch> batches;
                for (int i = 0; i < 3; ++i)
                {
                    auto rb = sp::record_batch(
                        {{"int_col", sp::array(sp::primitive_array<int32_t>({i * 10}))},
                         {"string_col", sp::array(sp::string_array(std::vector<std::string>{"test"}))}}
                    );
                    batches.push_back(rb);
                }

                serializer << batches;

                CHECK_EQ(chunks.size(), 5);  // Schema + rb1 + 3 batches
                for (size_t i = 0; i < chunks.size(); ++i)
                {
                    CHECK_GT(chunks[i].size(), 0);
                }
            }

            SUBCASE("Chaining range and single batch appends")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);

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

                serializer << batches << rb2;

                CHECK_EQ(chunks.size(), 5);  // Schema + rb1 + 2 from range + rb2
            }

            SUBCASE("Mixed chaining with multiple ranges")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);

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

                serializer << batches1 << rb2 << batches2;

                CHECK_EQ(chunks.size(), 5);  // Schema + rb1 + 1 from batches1 + rb2 + 1 from batches2
            }

            SUBCASE("Cannot use << with range after end")
            {
                auto rb1 = create_test_record_batch();
                std::vector<std::vector<uint8_t>> chunks;
                chunked_memory_output_stream stream(chunks);

                chunk_serializer serializer(stream);
                serializer.write(rb1);
                serializer.end();

                std::vector<sp::record_batch> batches = {create_test_record_batch()};
                CHECK_THROWS_AS(serializer << batches, std::runtime_error);
            }
        }
    }
}
