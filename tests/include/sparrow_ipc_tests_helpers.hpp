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
}
