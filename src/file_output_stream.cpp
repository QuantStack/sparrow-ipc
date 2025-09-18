#include "sparrow_ipc/file_output_stream.hpp"

namespace sparrow_ipc
{
    file_output_stream::file_output_stream(std::ofstream& file)
        : m_file(file)
    {
        if (!m_file.is_open())
        {
            throw std::runtime_error("Failed to open file stream");
        }
    }

    std::size_t file_output_stream::write(std::span<const std::uint8_t> span)
    {
        m_file.write(reinterpret_cast<const char*>(span.data()), span.size());
        m_written_bytes += span.size();
        return span.size();
    }

    std::size_t file_output_stream::write(uint8_t value, std::size_t count)
    {
        std::fill_n(std::ostreambuf_iterator<char>(m_file), count, value);
        m_written_bytes += count;
        return count;
    }

    size_t file_output_stream::size() const
    {
        return m_written_bytes;
    }

    void file_output_stream::reserve(std::size_t size)
    {
        // File streams do not support reservation
    }

    void file_output_stream::reserve(const std::function<std::size_t()>& calculate_reserve_size)
    {
        // File streams do not support reservation
    }

    void file_output_stream::flush()
    {
        m_file.flush();
    }

    void file_output_stream::close()
    {
        m_file.close();
    }

    bool file_output_stream::is_open() const
    {
        return m_file.is_open();
    }
}