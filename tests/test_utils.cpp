#include <doctest/doctest.h>

#include "sparrow_ipc/utils.hpp"


namespace sparrow_ipc
{
    TEST_CASE("align_to_8")
    {
        CHECK_EQ(utils::align_to_8(0), 0);
        CHECK_EQ(utils::align_to_8(1), 8);
        CHECK_EQ(utils::align_to_8(7), 8);
        CHECK_EQ(utils::align_to_8(8), 8);
        CHECK_EQ(utils::align_to_8(9), 16);
        CHECK_EQ(utils::align_to_8(15), 16);
        CHECK_EQ(utils::align_to_8(16), 16);
    }
}
