#include "sparrow_ipc/magic_values.hpp"

namespace sparrow_ipc
{
    bool is_continuation(std::istream& stream)
    {
        std::array<uint8_t, 4> buf;
        stream.read(reinterpret_cast<char*>(buf.data()), 4);
        if (stream.gcount() < 4)
        {
            if (stream.eof())
            {
                return false;  // End of file reached, not a continuation
            }
            throw std::runtime_error("Failed to read enough bytes from stream.");
        }
        return is_continuation(buf);
    }
}