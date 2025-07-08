#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#include <iostream>

#include "doctest/doctest.h"
#include "sparrow/sparrow.hpp"

#include "../include/serialize.hpp"

// Helper function to compare ArrowSchema structs
void compare_arrow_schemas(const ArrowSchema& s1, const ArrowSchema& s2)
{
    // Compare format
    std::string_view s1_format = (s1.format != nullptr) ? std::string_view(s1.format) : "";
    std::string_view s2_format = (s2.format != nullptr) ? std::string_view(s2.format) : "";

    std::cout << "s1_format "  << s1_format << " s2_format " << s2_format<< std::endl;
    CHECK_EQ(s1_format, s2_format);

    // Compare name (handle nullptr for name)
    std::string_view s1_name = (s1.name != nullptr) ? std::string_view(s1.name) : "";
    std::string_view s2_name = (s2.name != nullptr) ? std::string_view(s2.name) : "";
    std::cout << "s1 null ? "  << (s1.name == nullptr) << " s2 null? " << (s2.name == nullptr) << std::endl;
    std::cout << "s1: "  << s1_name << " s2: " << s2_name<< std::endl;
    CHECK_EQ(s1_name, s2_name);

    // Compare metadata
    if (s1.metadata == nullptr)
    {
        CHECK_EQ(s2.metadata, nullptr);
    }
    else
    {
        REQUIRE_NE(s2.metadata, nullptr);
//         CHECK_EQ(s1.metadata, s2.metadata);
//         sparrow::key_value_view kvs1_view(s1.metadata);
//         sparrow::key_value_view kvs2_view(s2.metadata);
//
//         std::vector<std::pair<std::string, std::string>> kvs1, kvs2;
//         for (const auto& kv : kvs1_view)
//         {
//             kvs1.emplace_back(kv.first, kv.second);
//         }
//         for (const auto& kv : kvs2_view)
//         {
//             kvs2.emplace_back(kv.first, kv.second);
//         }
//
//         // Sorting to ensure order doesn't matter for comparison
//         std::sort(kvs1.begin(), kvs1.end());
//         std::sort(kvs2.begin(), kvs2.end());
//
//         CHECK_EQ(kvs1, kvs2);
//
//         for (const auto& pair : kvs1)
//         {
//             std::cout << "  Key1: \"" << pair.first << "\", Value1: \"" << pair.second << "\"" << std::endl;
//         }
//         for (const auto& pair : kvs2)
//         {
//             std::cout << "  Key2: \"" << pair.first << "\", Value2: \"" << pair.second << "\"" << std::endl;
//         }
    }

    // Compare flags
    CHECK_EQ(s1.flags, s2.flags);

    // Compare number of children
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

    std::cout << "SER DATA SIZE: " << serialized_data.size() << std::endl;

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
}

TEST_CASE("Serialize and Deserialize primitive_array - float")
{
    namespace sp = sparrow;

    sp::primitive_array<float> ar = {10.5f, 20.5f, 30.5f, 40.5f, 50.5f};

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    std::cout << "SER DATA SIZE (float): " << serialized_data.size() << std::endl;

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<float> deserialized_ar = deserialize_primitive_array<float>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
}

TEST_CASE("Serialize and Deserialize primitive_array - double")
{
    namespace sp = sparrow;

    sp::primitive_array<double> ar = {10.1, 20.2, 30.3, 40.4, 50.5};

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    std::cout << "SER DATA SIZE (double): " << serialized_data.size() << std::endl;

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<double> deserialized_ar = deserialize_primitive_array<double>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
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

    std::cout << "SER DATA SIZE (int with nulls): " << serialized_data.size() << std::endl;

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);
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
        "my_named_array", // Name
        //std::make_optional(sp::get_metadata_from_key_values(metadata)) // Metadata
        //std::make_optional(metadata) // Metadata
        std::make_optional(std::vector<sparrow::metadata_pair>{{"key1", "value1"}, {"key2", "value2"}})
    );

    std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

    std::cout << "SER DATA SIZE (with name and metadata): " << serialized_data.size() << std::endl;

    CHECK(serialized_data.size() > 0);

    sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

    CHECK_EQ(ar, deserialized_ar);

    auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
    auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

    // Check ArrowSchema equality
    compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);

/////////////////////////////////////////////////////////////
    // TO PUT IN A FCT

        sparrow::key_value_view kvs1_view = *(ar.metadata());
        sparrow::key_value_view kvs2_view = *(deserialized_ar.metadata());

        std::vector<std::pair<std::string, std::string>> kvs1, kvs2;
        auto kvs1_it = kvs1_view.cbegin();
        std::cout << "============> SIZE: " << kvs1_view.size() << " and "  << kvs2_view.size() << std::endl;
        for (auto i = 0; i < kvs1_view.size(); ++i, ++kvs1_it)
        {
            std::cout << "ksv1 first: " << (*kvs1_it).first <<  " ksv1 sec: " << (*kvs1_it).second << std::endl;
        }
}
