#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#include "doctest/doctest.h"
#include "sparrow/sparrow.hpp"

#include "../include/serialize.hpp"

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

void compare_arrow_schemas(const ArrowSchema& s1, const ArrowSchema& s2)
{
    std::string_view s1_format = (s1.format != nullptr) ? std::string_view(s1.format) : "";
    std::string_view s2_format = (s2.format != nullptr) ? std::string_view(s2.format) : "";
    CHECK_EQ(s1_format, s2_format);

    std::string_view s1_name = (s1.name != nullptr) ? std::string_view(s1.name) : "";
    std::string_view s2_name = (s2.name != nullptr) ? std::string_view(s2.name) : "";
    CHECK_EQ(s1_name, s2_name);

    if (s1.metadata == nullptr)
    {
        CHECK_EQ(s2.metadata, nullptr);
    }
    else
    {
        REQUIRE_NE(s2.metadata, nullptr);
    }

    CHECK_EQ(s1.flags, s2.flags);
    CHECK_EQ(s1.n_children, s2.n_children);

    if (s1.n_children > 0)
    {
        REQUIRE_NE(s1.children, nullptr);
        REQUIRE_NE(s2.children, nullptr);
        for (int64_t i = 0; i < s1.n_children; ++i)
        {
            REQUIRE_NE(s1.children[i], nullptr);
            REQUIRE_NE(s2.children[i], nullptr);
            compare_arrow_schemas(*s1.children[i], *s2.children[i]);
        }
    }
    else
    {
        CHECK_EQ(s1.children, nullptr);
        CHECK_EQ(s2.children, nullptr);
    }

    if (s1.dictionary != nullptr)
    {
        REQUIRE_NE(s2.dictionary, nullptr);
        compare_arrow_schemas(*s1.dictionary, *s2.dictionary);
    }
    else
    {
        CHECK_EQ(s2.dictionary, nullptr);
    }
}


TEST_CASE("Serialize and Deserialize primitive_array - int")
{
    namespace sp = sparrow;

    sp::primitive_array<int> ar = {10, 20, 30, 40, 50};

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    REQUIRE_NE(arrow_schema_ar, nullptr);
    REQUIRE_NE(arrow_schema_deserialized_ar, nullptr);
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
    compare_bitmap<int>(ar, deserialized_ar);
    compare_metadata<int>(ar, deserialized_ar);
}

TEST_CASE("Serialize and Deserialize primitive_array - float")
{
    namespace sp = sparrow;

    sp::primitive_array<float> ar = {10.5f, 20.5f, 30.5f, 40.5f, 50.5f};

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<float> deserialized_ar = deserialize_primitive_array<float>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    REQUIRE_NE(arrow_schema_ar, nullptr);
    REQUIRE_NE(arrow_schema_deserialized_ar, nullptr);
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
    compare_bitmap<float>(ar, deserialized_ar);
    compare_metadata<float>(ar, deserialized_ar);
}

TEST_CASE("Serialize and Deserialize primitive_array - double")
{
    namespace sp = sparrow;

    sp::primitive_array<double> ar = {10.1, 20.2, 30.3, 40.4, 50.5};

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<double> deserialized_ar = deserialize_primitive_array<double>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    REQUIRE_NE(arrow_schema_ar, nullptr);
    REQUIRE_NE(arrow_schema_deserialized_ar, nullptr);
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
    compare_bitmap<double>(ar, deserialized_ar);
    compare_metadata<double>(ar, deserialized_ar);
}

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

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    REQUIRE_NE(arrow_schema_ar, nullptr);
    REQUIRE_NE(arrow_schema_deserialized_ar, nullptr);
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
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

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    REQUIRE_NE(arrow_schema_ar, nullptr);
    REQUIRE_NE(arrow_schema_deserialized_ar, nullptr);
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
    compare_bitmap<int>(ar, deserialized_ar);
    compare_metadata<int>(ar, deserialized_ar);
}
