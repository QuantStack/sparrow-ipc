#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#include "doctest/doctest.h"
#include "sparrow.hpp"

#include "serialize_primitive_array.hpp"
#include "sparrow_ipc_tests_helpers.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;

    using testing_types = std::tuple<
        int,
        float,
        double>;

    template <typename T>
    void compare_bitmap(const sp::primitive_array<T>& pa1, const sp::primitive_array<T>& pa2)
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
    void compare_primitive_arrays(const sp::primitive_array<T>& ar, const sp::primitive_array<T>& deserialized_ar)
    {
        CHECK_EQ(ar, deserialized_ar);
        compare_bitmap<T>(ar, deserialized_ar);
        compare_metadata(ar, deserialized_ar);
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

        const std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

        CHECK(serialized_data.size() > 0);

        sp::primitive_array<T> deserialized_ar = deserialize_primitive_array<T>(serialized_data);

        compare_primitive_arrays<T>(ar, deserialized_ar);
    }

    TEST_CASE_TEMPLATE_APPLY(primitive_array_types, testing_types);

    TEST_CASE("Serialize and Deserialize primitive_array - int with nulls")
    {
        // Data buffer
        const sp::u8_buffer<int> data_buffer = {100, 200, 300, 400, 500};

        // Validity bitmap: 100 (valid), 200 (valid), 300 (null), 400 (valid), 500 (null)
        sp::validity_bitmap validity(5, true); // All valid initially
        validity.set(2, false); // Set index 2 to null
        validity.set(4, false); // Set index 4 to null

        sp::primitive_array<int> ar(std::move(data_buffer), std::move(validity));

        const std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

        CHECK(serialized_data.size() > 0);

        sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

        compare_primitive_arrays<int>(ar, deserialized_ar);
    }

    TEST_CASE("Serialize and Deserialize primitive_array - with name and metadata")
    {
        // Data buffer
        const sp::u8_buffer<int> data_buffer = {1, 2, 3};

        // Validity bitmap: All valid
        const sp::validity_bitmap validity(3, true);

        // Custom metadata
        const std::vector<sp::metadata_pair> metadata = {
            {"key1", "value1"},
            {"key2", "value2"}
        };

        sp::primitive_array<int> ar(
            std::move(data_buffer),
            std::move(validity),
            "my_named_array", // name
            std::make_optional(std::vector<sp::metadata_pair>{{"key1", "value1"}, {"key2", "value2"}})
        );

        const std::vector<uint8_t> serialized_data = serialize_primitive_array(ar);

        CHECK(serialized_data.size() > 0);

        sp::primitive_array<int> deserialized_ar = deserialize_primitive_array<int>(serialized_data);

        compare_primitive_arrays<int>(ar, deserialized_ar);
    }
}
