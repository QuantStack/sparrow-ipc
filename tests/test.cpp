#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#include "doctest/doctest.h"
#include "sparrow.hpp"

#include "../include/serialize.hpp"

using testing_types = std::tuple<
    int,
    float,
    double>;

template <typename T>
void compare_bitmap(sparrow::primitive_array<T>& pa1, sparrow::primitive_array<T>& pa2)
{
    const auto pa1_bitmap = pa1.bitmap();
    const auto pa2_bitmap = pa2.bitmap();

    CHECK_EQ(pa1_bitmap.size(), pa2_bitmap.size());
    auto pa1_it = pa1_bitmap.begin();
    auto pa2_it = pa2_bitmap.begin();
    for (size_t i = 0; i < pa1_bitmap.size(); ++i)
    {
        CHECK_EQ(*pa1_it, *pa2_it);
        ++pa1_it;
        ++pa2_it;
    }
}

template <typename T>
void compare_metadata(sparrow::primitive_array<T>& pa1, sparrow::primitive_array<T>& pa2)
{
    if (!pa1.metadata().has_value())
    {
        CHECK(!pa2.metadata().has_value());
        return;
    }

    CHECK(pa2.metadata().has_value());
    sparrow::key_value_view kvs1_view = *(pa1.metadata());
    sparrow::key_value_view kvs2_view = *(pa2.metadata());

    CHECK_EQ(kvs1_view.size(), kvs2_view.size());
    std::vector<std::pair<std::string, std::string>> kvs1, kvs2;
    auto kvs1_it = kvs1_view.cbegin();
    auto kvs2_it = kvs2_view.cbegin();
    for (auto i = 0; i < kvs1_view.size(); ++i)
    {
        CHECK_EQ(*kvs1_it, *kvs2_it);
        ++kvs1_it;
        ++kvs2_it;
    }
}

TEST_CASE_TEMPLATE_DEFINE("Serialize and Deserialize primitive_array", T, primitive_array_types)
{
    namespace sp = sparrow;

    auto create_primitive_array = []() -> sp::primitive_array<T> {
        if constexpr (std::is_same_v<T, int>)
        {
            return {10, 20, 30, 40, 50};
        }
        else if constexpr (std::is_same_v<T, float>)
        {
            return {10.5f, 20.5f, 30.5f, 40.5f, 50.5f};
        }
        else if constexpr (std::is_same_v<T, double>)
        {
            return {10.1, 20.2, 30.3, 40.4, 50.5};
        }
        else
        {
            FAIL("Unsupported type for templated test case");
        }
    };

    sp::primitive_array<T> ar = create_primitive_array();

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<T> deserialized_ar = deserialize_primitive_array<T>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    compare_bitmap<T>(ar, deserialized_ar);
    compare_metadata<T>(ar, deserialized_ar);
}

TEST_CASE_TEMPLATE_APPLY(primitive_array_types, testing_types);

TEST_CASE("Serialize and Deserialize primitive_array - int with nulls")
{
    namespace sp = sparrow;

    // Data buffer
    sp::u8_buffer<int> data_buffer = {100, 200, 300, 400, 500};

    // Validity bitmap: 100 (valid), 200 (valid), 300 (null), 400 (valid), 500 (null)
    sp::validity_bitmap validity(5, true); // All valid initially
    validity.set(2, false); // Set index 2 to null
    validity.set(4, false); // Set index 4 to null

    sp::primitive_array<int> ar(std::move(data_buffer), std::move(validity));

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    compare_bitmap<int>(ar, deserialized_ar);
    compare_metadata<int>(ar, deserialized_ar);
}

TEST_CASE("Serialize and Deserialize primitive_array - with name and metadata")
{
    namespace sp = sparrow;

    // Data buffer
    sp::u8_buffer<int> data_buffer = {1, 2, 3};

    // Validity bitmap: All valid
    sp::validity_bitmap validity(3, true);

    // Custom metadata
    std::vector<sp::metadata_pair> metadata = {
        {"key1", "value1"},
        {"key2", "value2"}
    };

    sp::primitive_array<int> ar(
        std::move(data_buffer),
        std::move(validity),
        "my_named_array", // name
        std::make_optional(std::vector<sparrow::metadata_pair>{{"key1", "value1"}, {"key2", "value2"}})
    );

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    compare_bitmap<int>(ar, deserialized_ar);
    compare_metadata<int>(ar, deserialized_ar);
}
