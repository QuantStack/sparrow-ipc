#include "sparrow_ipc/arrow_interface/arrow_schema/private_data.hpp"

namespace sparrow_ipc
{
    arrow_schema_private_data::arrow_schema_private_data(
        std::string_view format,
        const char* name,
        std::optional<std::string> metadata
    )
        : m_format(format)
        , m_name(name)
        , m_metadata(std::move(metadata))
    {
    }

    const char* arrow_schema_private_data::format_ptr() const noexcept
    {
        return m_format.data();
    }

    const char* arrow_schema_private_data::name_ptr() const noexcept
    {
        return m_name;
    }

    const char* arrow_schema_private_data::metadata_ptr() const noexcept
    {
        return m_metadata.has_value() ? m_metadata->c_str() : nullptr;
    }
}