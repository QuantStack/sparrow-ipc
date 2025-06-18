#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN

#include "sparrow/sparrow.hpp"
#include "doctest/doctest.h"

#include "../generated/Schema_generated.h"

// NOTE this is just testing sparrow internals usability,
// for now we are not testing anything with serialization/deserialization
TEST_CASE("Use sparrow primitive_array")
{
    namespace sp = sparrow;

    sp::primitive_array<int> ar = { 1, 3, 5, 7, 9 };
    CHECK_EQ(ar.size(), 5);

    auto [arrow_array, arrow_schema] = sp::extract_arrow_structures(std::move(ar));
    CHECK_EQ(arrow_array.length, 5);

    // Serialize
    // Deserialize

    arrow_array.release(&arrow_array);
    arrow_schema.release(&arrow_schema);
}
