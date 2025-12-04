
#pragma once

#include <sparrow/c_interface.hpp>

#include "arrow_array/private_data.hpp"
#include "arrow_schema/private_data.hpp"

namespace sparrow_ipc
{
    // TODO Find a way to use sparrow internals directly and avoid duplicated code
    /**
     * Release the children and dictionnary of an `ArrowArray` or `ArrowSchema`.
     *
     * @tparam T `ArrowArray` or `ArrowSchema`
     * @param t The `ArrowArray` or `ArrowSchema` to release.
     */
    template <class T>
        requires std::same_as<T, ArrowArray> || std::same_as<T, ArrowSchema>
    void release_common_non_owning_arrow(T& t)
    {
        using private_data_type = std::conditional_t<
            std::same_as<T, ArrowArray>,
            arrow_array_private_data,
            non_owning_arrow_schema_private_data>;
        if (t.release == nullptr)
        {
            return;
        }
        SPARROW_ASSERT_TRUE(t.private_data != nullptr);
        const auto private_data = static_cast<const private_data_type*>(t.private_data);
        delete private_data;
        t.private_data = nullptr;

        if (t.dictionary)
        {
            if (t.dictionary->release)
            {
                t.dictionary->release(t.dictionary);
            }
            delete t.dictionary;
            t.dictionary = nullptr;
        }

        if (t.children)
        {
            for (int64_t i = 0; i < t.n_children; ++i)
            {
                T* child = t.children[i];
                if (child)
                {
                    if (child->release)
                    {
                        child->release(child);
                    }
                    delete child;
                    child = nullptr;
                }
            }
            delete[] t.children;
            t.children = nullptr;
        }
        t.release = nullptr;
    }
}
