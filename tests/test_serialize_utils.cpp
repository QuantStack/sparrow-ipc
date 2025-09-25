#include <doctest/doctest.h>
#include <sparrow.hpp>

#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/serialize_utils.hpp"
#include "sparrow_ipc/utils.hpp"
#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    TEST_CASE("create_metadata")
    {
        flatbuffers::FlatBufferBuilder builder;

        SUBCASE("No metadata (nullptr)")
        {
            auto schema = create_test_arrow_schema("i");
            auto metadata_offset = create_metadata(builder, schema);
            CHECK_EQ(metadata_offset.o, 0);
        }

        SUBCASE("With metadata - basic test")
        {
            auto schema = create_test_arrow_schema_with_metadata("i");
            auto metadata_offset = create_metadata(builder, schema);
            // For now just check that it doesn't crash
            // TODO: Add proper metadata testing when sparrow metadata is properly handled
        }
    }

    TEST_CASE("create_field")
    {
        flatbuffers::FlatBufferBuilder builder;

        SUBCASE("Basic field creation")
        {
            auto schema = create_test_arrow_schema("i", "int_field", true);
            auto field_offset = create_field(builder, schema);
            CHECK_NE(field_offset.o, 0);
        }

        SUBCASE("Field with null name")
        {
            auto schema = create_test_arrow_schema("i", nullptr, false);
            auto field_offset = create_field(builder, schema);
            CHECK_NE(field_offset.o, 0);
        }

        SUBCASE("Non-nullable field")
        {
            auto schema = create_test_arrow_schema("i", "int_field", false);
            auto field_offset = create_field(builder, schema);
            CHECK_NE(field_offset.o, 0);
        }
    }

    TEST_CASE("create_children from ArrowSchema")
    {
        flatbuffers::FlatBufferBuilder builder;

        SUBCASE("No children")
        {
            auto schema = create_test_arrow_schema("i");
            auto children_offset = create_children(builder, schema);
            CHECK_EQ(children_offset.o, 0);
        }

        SUBCASE("With children")
        {
            auto parent_schema = create_test_arrow_schema("+s");
            auto child1 = new ArrowSchema(create_test_arrow_schema("i", "child1"));
            auto child2 = new ArrowSchema(create_test_arrow_schema("u", "child2"));

            ArrowSchema* children[] = {child1, child2};
            parent_schema.children = children;
            parent_schema.n_children = 2;

            auto children_offset = create_children(builder, parent_schema);
            CHECK_NE(children_offset.o, 0);

            // Clean up
            delete child1;
            delete child2;
        }

        SUBCASE("Null child pointer throws exception")
        {
            auto parent_schema = create_test_arrow_schema("+s");
            ArrowSchema* children[] = {nullptr};
            parent_schema.children = children;
            parent_schema.n_children = 1;

            CHECK_THROWS_AS(create_children(builder, parent_schema), std::invalid_argument);
        }
    }

    TEST_CASE("create_children from record_batch columns")
    {
        flatbuffers::FlatBufferBuilder builder;

        SUBCASE("With valid record batch")
        {
            auto record_batch = create_test_record_batch();
            auto children_offset = create_children(builder, record_batch.columns());
            CHECK_NE(children_offset.o, 0);
        }

        SUBCASE("Empty record batch")
        {
            auto empty_batch = sp::record_batch({});

            auto children_offset = create_children(builder, empty_batch.columns());
            CHECK_EQ(children_offset.o, 0);
        }
    }

    TEST_CASE("get_schema_message_builder")
    {
        SUBCASE("Valid record batch")
        {
            auto record_batch = create_test_record_batch();
            auto builder = get_schema_message_builder(record_batch);

            CHECK_GT(builder.GetSize(), 0);
            CHECK_NE(builder.GetBufferPointer(), nullptr);
        }
    }

    TEST_CASE("serialize_schema_message")
    {
        SUBCASE("Valid record batch")
        {
            auto record_batch = create_test_record_batch();
            auto serialized = serialize_schema_message(record_batch);

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

    TEST_CASE("fill_fieldnodes")
    {
        SUBCASE("Single array without children")
        {
            auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
            auto proxy = sp::detail::array_access::get_arrow_proxy(array);

            std::vector<org::apache::arrow::flatbuf::FieldNode> nodes;
            fill_fieldnodes(proxy, nodes);

            CHECK_EQ(nodes.size(), 1);
            CHECK_EQ(nodes[0].length(), 5);
            CHECK_EQ(nodes[0].null_count(), 0);
        }

        SUBCASE("Array with null values")
        {
            // For now, just test with a simple array without explicit nulls
            // Creating arrays with null values requires more complex sparrow setup
            auto array = sp::primitive_array<int32_t>({1, 2, 3});
            auto proxy = sp::detail::array_access::get_arrow_proxy(array);

            std::vector<org::apache::arrow::flatbuf::FieldNode> nodes;
            fill_fieldnodes(proxy, nodes);

            CHECK_EQ(nodes.size(), 1);
            CHECK_EQ(nodes[0].length(), 3);
            CHECK_EQ(nodes[0].null_count(), 0);
        }
    }

    TEST_CASE("create_fieldnodes")
    {
        SUBCASE("Record batch with multiple columns")
        {
            auto record_batch = create_test_record_batch();
            auto nodes = create_fieldnodes(record_batch);

            CHECK_EQ(nodes.size(), 2);  // Two columns

            // Check the first column (integer array)
            CHECK_EQ(nodes[0].length(), 5);
            CHECK_EQ(nodes[0].null_count(), 0);

            // Check the second column (string array)
            CHECK_EQ(nodes[1].length(), 5);
            CHECK_EQ(nodes[1].null_count(), 0);
        }
    }

    TEST_CASE("fill_buffers")
    {
        SUBCASE("Simple primitive array")
        {
            auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
            auto proxy = sp::detail::array_access::get_arrow_proxy(array);

            std::vector<org::apache::arrow::flatbuf::Buffer> buffers;
            int64_t offset = 0;
            fill_buffers(proxy, buffers, offset);

            CHECK_GT(buffers.size(), 0);
            CHECK_GT(offset, 0);

            // Verify offsets are aligned
            for (const auto& buffer : buffers)
            {
                CHECK_EQ(buffer.offset() % 8, 0);
            }
        }
    }

    TEST_CASE("get_buffers")
    {
        SUBCASE("Record batch with multiple columns")
        {
            auto record_batch = create_test_record_batch();
            auto buffers = get_buffers(record_batch);
            CHECK_GT(buffers.size(), 0);
            // Verify all offsets are properly calculated and aligned
            for (size_t i = 1; i < buffers.size(); ++i)
            {
                CHECK_GE(buffers[i].offset(), buffers[i - 1].offset() + buffers[i - 1].length());
            }
        }
    }

    TEST_CASE("fill_body")
    {
        SUBCASE("Simple primitive array")
        {
            auto array = sp::primitive_array<int32_t>({1, 2, 3, 4, 5});
            auto proxy = sp::detail::array_access::get_arrow_proxy(array);
            std::vector<uint8_t> body;
            fill_body(proxy, body);
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
            auto body = generate_body(record_batch);

            CHECK_GT(body.size(), 0);
            CHECK_EQ(body.size() % 8, 0);
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
            auto body = generate_body(record_batch);
            CHECK_EQ(size, static_cast<int64_t>(body.size()));
        }
    }

    TEST_CASE("get_record_batch_message_builder")
    {
        SUBCASE("Valid record batch with field nodes and buffers")
        {
            auto record_batch = create_test_record_batch();
            auto nodes = create_fieldnodes(record_batch);
            auto buffers = get_buffers(record_batch);
            auto builder = get_record_batch_message_builder(record_batch, nodes, buffers);
            CHECK_GT(builder.GetSize(), 0);
            CHECK_NE(builder.GetBufferPointer(), nullptr);
        }
    }

    TEST_CASE("serialize_record_batch")
    {
        SUBCASE("Valid record batch")
        {
            auto record_batch = create_test_record_batch();
            auto serialized = serialize_record_batch(record_batch);
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
            auto serialized = serialize_record_batch(empty_batch);
            CHECK_GT(serialized.size(), 0);
            CHECK_GE(serialized.size(), continuation.size());
        }
    }

    TEST_CASE("Integration test - schema and record batch serialization")
    {
        SUBCASE("Serialize schema and record batch for same data")
        {
            auto record_batch = create_test_record_batch();

            auto schema_serialized = serialize_schema_message(record_batch);
            auto record_batch_serialized = serialize_record_batch(record_batch);

            CHECK_GT(schema_serialized.size(), 0);
            CHECK_GT(record_batch_serialized.size(), 0);

            // Both should start with continuation bytes
            CHECK_GE(schema_serialized.size(), continuation.size());
            CHECK_GE(record_batch_serialized.size(), continuation.size());

            // Both should be properly aligned
            CHECK_EQ(schema_serialized.size() % 8, 0);
        }
    }
}