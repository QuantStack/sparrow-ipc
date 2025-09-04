#include "sparrow_ipc/arrow_interface/arrow_schema.hpp"

#include "sparrow_ipc/arrow_interface/arrow_array_schema_common_release.hpp"

namespace sparrow_ipc
{
    void release_non_owning_arrow_schema(ArrowSchema* schema)
    {
        SPARROW_ASSERT_FALSE(schema == nullptr);
        SPARROW_ASSERT_TRUE(schema->release == std::addressof(release_non_owning_arrow_schema));
        release_common_non_owning_arrow(*schema);
        if (schema->private_data != nullptr)
        {
            const auto private_data = static_cast<non_owning_arrow_schema_private_data*>(schema->private_data);
            delete private_data;
            schema->private_data = nullptr;
        }
        *schema = {};
    }
}