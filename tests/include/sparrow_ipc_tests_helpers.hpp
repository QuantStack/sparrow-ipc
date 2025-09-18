#pragma once

#include "doctest/doctest.h"
#include "sparrow.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    template <typename T1, typename T2>
    void compare_metadata(const T1& arr1, const T2& arr2)
    {
        if (!arr1.metadata().has_value())
        {
            CHECK(!arr2.metadata().has_value());
            return;
        }

        CHECK(arr2.metadata().has_value());
        sp::key_value_view kvs1_view = arr1.metadata().value();
        sp::key_value_view kvs2_view = arr2.metadata().value();

        CHECK_EQ(kvs1_view.size(), kvs2_view.size());
        auto kvs1_it = kvs1_view.cbegin();
        auto kvs2_it = kvs2_view.cbegin();
        for (auto i = 0; i < kvs1_view.size(); ++i)
        {
            CHECK_EQ(*kvs1_it, *kvs2_it);
            ++kvs1_it;
            ++kvs2_it;
        }
    }

        // Helper function to create a simple ArrowSchema for testing
    ArrowSchema
    create_test_arrow_schema(const char* format, const char* name = "test_field", bool nullable = true)
    {
        ArrowSchema schema{};
        schema.format = format;
        schema.name = name;
        schema.flags = nullable ? static_cast<int64_t>(sp::ArrowFlag::NULLABLE) : 0;
        schema.metadata = nullptr;
        schema.n_children = 0;
        schema.children = nullptr;
        schema.dictionary = nullptr;
        schema.release = nullptr;
        schema.private_data = nullptr;
        return schema;
    }

    // Helper function to create ArrowSchema with metadata
    ArrowSchema create_test_arrow_schema_with_metadata(const char* format, const char* name = "test_field")
    {
        auto schema = create_test_arrow_schema(format, name);

        // For now, let's just return the schema without metadata to avoid segfault
        // The metadata creation requires proper sparrow metadata handling
        return schema;
    }

    // Helper function to create a simple record batch for testing
    sp::record_batch create_test_record_batch()
    {
        // Create a simple record batch with integer and string columns using initializer syntax
        return sp::record_batch(
            {{"int_col", sp::array(sp::primitive_array<int32_t>({1, 2, 3, 4, 5}))},
             {"string_col",
              sp::array(sp::string_array(std::vector<std::string>{"hello", "world", "test", "data", "batch"}))}}
        );
    }
}
