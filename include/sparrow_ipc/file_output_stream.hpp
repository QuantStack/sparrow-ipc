#include <fstream>
#include <functional>

#include "sparrow_ipc/output_stream.hpp"


namespace sparrow_ipc
{
    class SPARROW_IPC_API file_output_stream final : public output_stream
    {
    public:

        explicit file_output_stream(std::ofstream& file);

        std::size_t write(std::span<const std::uint8_t> span) override;

        std::size_t write(uint8_t value, std::size_t count = 1) override;

        size_t size() const override;

        void reserve(std::size_t size) override;

        void reserve(const std::function<std::size_t()>& calculate_reserve_size) override;

        void flush() override;

        void close() override;

        bool is_open() const override;

    private:

        std::ofstream& m_file;
        size_t m_written_bytes = 0;
    };
}