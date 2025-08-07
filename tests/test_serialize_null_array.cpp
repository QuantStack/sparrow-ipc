#include "doctest/doctest.h"
#include "sparrow.hpp"

#include "serialize_null_array.hpp"

namespace sparrow_ipc
{
    namespace sp = sparrow;
    // TODO have a generic compare_metadata in tests/helpers.hpp, cpp
    // taking pa and na
    void compare_metadata(sp::null_array& na1, sp::null_array& na2)
    {
        if (!na1.metadata().has_value())
        {
            CHECK(!na2.metadata().has_value());
            return;
        }

        CHECK(na2.metadata().has_value());
        sp::key_value_view kvs1_view = *(na1.metadata());
        sp::key_value_view kvs2_view = *(na2.metadata());

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

    TEST_CASE("Serialize and deserialize null_array")
    {
        const std::size_t size = 10;
        const std::string_view name = "my_null_array";

        const std::vector<sp::metadata_pair> metadata_vec = {{"key1", "value1"}, {"key2", "value2"}};
        const std::optional<std::vector<sp::metadata_pair>> metadata = metadata_vec;

        sp::null_array arr(size, name, metadata);

        auto buffer = serialize_null_array(arr);
        auto deserialized_arr = deserialize_null_array(buffer);

        CHECK_EQ(deserialized_arr.size(), arr.size());
        REQUIRE(deserialized_arr.name().has_value());
        CHECK_EQ(deserialized_arr.name().value(), arr.name().value());

        REQUIRE(deserialized_arr.metadata().has_value());
        compare_metadata(arr, deserialized_arr);

        // Check the deserialized object is a null_array
        const auto& arrow_proxy = sp::detail::array_access::get_arrow_proxy(deserialized_arr);
        CHECK_EQ(arrow_proxy.format(), "n");
        CHECK_EQ(arrow_proxy.n_children(), 0);
        CHECK_EQ(arrow_proxy.flags(), std::unordered_set<sp::ArrowFlag>{sp::ArrowFlag::NULLABLE});
        CHECK_EQ(arrow_proxy.name(), name);
        CHECK_EQ(arrow_proxy.dictionary(), nullptr);
        CHECK_EQ(arrow_proxy.buffers().size(), 0);
    }

    TEST_CASE("Serialize and deserialize null_array with no name and no metadata")
    {
        const std::size_t size = 100;
        sp::null_array arr(size);
        auto buffer = serialize_null_array(arr);
        auto deserialized_arr = deserialize_null_array(buffer);
        CHECK_EQ(deserialized_arr.size(), arr.size());
        CHECK_FALSE(deserialized_arr.name().has_value());
        CHECK_FALSE(deserialized_arr.metadata().has_value());
    }
}
