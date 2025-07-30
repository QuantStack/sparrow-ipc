#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#include "doctest/doctest.h"
#include "sparrow.hpp"

#include "serialize.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    using testing_types = std::tuple<
        int,
        float,
        double>;

    // TODO We should use comparison functions from sparrow, after making them available if not already
    // after next release?
    // Or even better, allow checking directly primitive_array equality in sparrow
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

    void compare_arrow_arrays(const ArrowArray& lhs, const ArrowArray& rhs)
    {
        CHECK_EQ(lhs.length, rhs.length);
        CHECK_EQ(lhs.null_count, rhs.null_count);
        CHECK_EQ(lhs.offset, rhs.offset);
        CHECK_EQ(lhs.n_buffers, rhs.n_buffers);
        CHECK_EQ(lhs.n_children, rhs.n_children);
        CHECK_NE(lhs.buffers, rhs.buffers);
        CHECK_NE(lhs.private_data, rhs.private_data);
        for (size_t i = 0; i < static_cast<size_t>(lhs.n_buffers); ++i)
        {
            CHECK_NE(lhs.buffers[i], rhs.buffers[i]);
        }
        auto lhs_buffers = reinterpret_cast<const int8_t**>(lhs.buffers);
        auto rhs_buffers = reinterpret_cast<const int8_t**>(rhs.buffers);

        for (size_t i = 0; i < static_cast<size_t>(lhs.length); ++i)
        {
            CHECK_EQ(lhs_buffers[1][i], rhs_buffers[1][i]);
        }
    }

    template <typename T>
    void compare_values(sp::primitive_array<T>& pa1, sp::primitive_array<T>& pa2)
    {
        CHECK_EQ(pa1.size(), pa1.size());
        for (size_t i = 0; i < pa1.size(); ++i)
        {
            CHECK_EQ(pa1[i], pa2[i]);
        }
    }

    template <typename T>
    void compare_bitmap(sp::primitive_array<T>& pa1, sp::primitive_array<T>& pa2)
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
    void compare_metadata(sp::primitive_array<T>& pa1, sp::primitive_array<T>& pa2)
    {
        if (!pa1.metadata().has_value())
        {
            CHECK(!pa2.metadata().has_value());
            return;
        }

        CHECK(pa2.metadata().has_value());
        sp::key_value_view kvs1_view = *(pa1.metadata());
        sp::key_value_view kvs2_view = *(pa2.metadata());

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

    template <typename T>
    void compare_primitive_arrays(sp::primitive_array<T>& ar, sp::primitive_array<T>& deserialized_ar)
    {
        auto [arrow_array_ar, arrow_schema_ar] = sp::get_arrow_structures(ar);
        auto [arrow_array_deserialized_ar, arrow_schema_deserialized_ar] = sp::get_arrow_structures(deserialized_ar);

        // Check ArrowSchema equality
        REQUIRE_NE(arrow_schema_ar, nullptr);
        REQUIRE_NE(arrow_schema_deserialized_ar, nullptr);
        compare_arrow_schemas(*arrow_schema_ar, *arrow_schema_deserialized_ar);

        // Check ArrowArray equality
        REQUIRE_NE(arrow_array_ar, nullptr);
        REQUIRE_NE(arrow_array_deserialized_ar, nullptr);
        compare_arrow_arrays(*arrow_array_ar, *arrow_array_deserialized_ar);

//         compare_values<T>(ar, deserialized_ar);
        compare_bitmap<T>(ar, deserialized_ar);
        compare_metadata<T>(ar, deserialized_ar);
    }

    TEST_CASE_TEMPLATE_DEFINE("Serialize and Deserialize primitive_array", T, primitive_array_types)
    {
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

        compare_primitive_arrays<T>(ar, deserialized_ar);
    }

    TEST_CASE_TEMPLATE_APPLY(primitive_array_types, testing_types);

    TEST_CASE("Serialize and Deserialize primitive_array - int with nulls")
    {
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

        compare_primitive_arrays<int>(ar, deserialized_ar);
    }

    TEST_CASE("Serialize and Deserialize primitive_array - with name and metadata")
    {
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
            std::make_optional(std::vector<sp::metadata_pair>{{"key1", "value1"}, {"key2", "value2"}})
        );

        std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

        CHECK(serialized_data.size() > 0);

        sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

        compare_primitive_arrays<int>(ar, deserialized_ar);
    }
}
