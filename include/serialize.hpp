#pragma once

#include <vector>
#include "sparrow/sparrow.hpp"

template <typename T>
std::vector<uint8_t> serialize_primitive_array(const sparrow::primitive_array<T>& arr);

template <typename T>
sparrow::primitive_array<T> deserialize_primitive_array(const std::vector<uint8_t>& buffer);
