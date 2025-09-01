
#pragma once

#include <optional>
#include <string>

namespace sparrow_ipc
{
    class arrow_schema_private_data
    {
    public:

        arrow_schema_private_data(std::string_view format, const char* name, std::optional<std::string> metadata);

        [[nodiscard]] const char* format_ptr() const noexcept;
        [[nodiscard]] const char* name_ptr() const noexcept;
        [[nodiscard]] const char* metadata_ptr() const noexcept;

    private:

        std::string m_format;
        const char* m_name;
        std::optional<std::string> m_metadata;
    };
}
