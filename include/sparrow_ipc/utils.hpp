#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>

#include <sparrow/record_batch.hpp>

#include "Schema_generated.h"
#include "sparrow_ipc/config/config.hpp"

namespace sparrow_ipc::utils
{
    // Aligns a value to the next multiple of 8, as required by the Arrow IPC format for message bodies
    SPARROW_IPC_API int64_t align_to_8(const int64_t n);

    // Creates a Flatbuffers type from a format string
    // This function maps a sparrow data type to the corresponding Flatbuffers type
    SPARROW_IPC_API std::pair<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>>
    get_flatbuffer_type(flatbuffers::FlatBufferBuilder& builder, std::string_view format_str);


    /**
     * @brief Checks if all record batches in a collection have consistent structure.
     * 
     * This function verifies that all record batches in the provided collection have:
     * - The same number of columns
     * - Matching data types for corresponding columns (same column index)
     * 
     * @tparam R Container type that holds sparrow::record_batch objects
     * @param record_batches Collection of record batches to check for consistency
     * @return true if all record batches have consistent structure or if the collection is empty,
     *         false if any structural inconsistencies are found
     * 
     * @note An empty collection is considered consistent and returns true
     * @note The number of rows per record batch is not required to be the same
     */
    template <std::ranges::input_range R>
        requires std::same_as<std::ranges::range_value_t<R>, sparrow::record_batch>
    bool check_record_batches_consistency(const R& record_batches)
    {
        if (record_batches.empty())
        {
            return true;
        }
        const sparrow::record_batch& first_rb = record_batches[0];
        const size_t first_rb_nb_rows = first_rb.nb_rows();
        const size_t first_rb_nb_columns = first_rb.nb_columns();
        for (const sparrow::record_batch& rb : record_batches)
        {
            const auto rb_nb_columns = rb.nb_columns();
            if (rb_nb_columns != first_rb_nb_columns)
            {
                return false;
            }
            for (size_t col_idx = 0; col_idx < rb.nb_columns(); ++col_idx)
            {
                const sparrow::array& arr = rb.get_column(col_idx);
                const sparrow::array& first_arr = first_rb.get_column(col_idx);
                const auto arr_data_type = arr.data_type();
                const auto first_arr_data_type = first_arr.data_type();
                if (arr_data_type != first_arr_data_type)
                {
                    return false;
                }
            }
        }
        return true;
    }

    // size_t calculate_output_serialized_size(const sparrow::record_batch& record_batch);
}
