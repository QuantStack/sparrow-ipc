#include "sparrow_ipc/arrow_interface/arrow_array.hpp"

#include <sparrow/c_interface.hpp>

namespace sparrow_ipc
{
    void release_arrow_array_children_and_dictionary(ArrowArray* array)
    {
        SPARROW_ASSERT_TRUE(array != nullptr)

        if (array->children)
        {
            for (int64_t i = 0; i < array->n_children; ++i)
            {
                ArrowArray* child = array->children[i];
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
            delete[] array->children;
            array->children = nullptr;
        }

        if (array->dictionary)
        {
            if (array->dictionary->release)
            {
                array->dictionary->release(array->dictionary);
            }
            delete array->dictionary;
            array->dictionary = nullptr;
        }
    }
}
