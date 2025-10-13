#pragma once

#include <cstdint>
#include <concepts>
#include <span>
#include <functional>
#include <ios>

namespace sparrow_ipc
{
    template <typename T>
    concept streamable = requires(T& t, const char* s, std::streamsize count )
    {
        { t.write(s, count) } -> std::same_as<T&>;

    };

    template <class T>
    concept reservable_stream = streamable<T> && requires(T& t, size_t size)
    {
        {t.reserve(size)} -> std::same_as<void>;
    };
}
