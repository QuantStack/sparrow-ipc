#pragma once

#include <vector>
#include "sparrow.hpp"

//TODO split serialize/deserialize fcts in two different files or just rename the current one?
template <typename T>
std::vector<uint8_t> serialize_primitive_array(const sparrow::primitive_array<T>& arr);

template <typename T>
sparrow::primitive_array<T> deserialize_primitive_array(const std::vector<uint8_t>& buffer);
