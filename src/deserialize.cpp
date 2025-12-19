#include "sparrow_ipc/deserialize.hpp"

#include <sparrow/buffer/buffer.hpp>
#include <sparrow/types/data_type.hpp>

#include "sparrow_ipc/deserialize_decimal_array.hpp"
#include "sparrow_ipc/deserialize_duration_array.hpp"
#include "sparrow_ipc/deserialize_fixedsizebinary_array.hpp"
#include "sparrow_ipc/deserialize_interval_array.hpp"
#include "sparrow_ipc/deserialize_null_array.hpp"
#include "sparrow_ipc/deserialize_primitive_array.hpp"
#include "sparrow_ipc/deserialize_variable_size_binary_array.hpp"
#include "sparrow_ipc/encapsulated_message.hpp"
#include "sparrow_ipc/magic_values.hpp"
#include "sparrow_ipc/metadata.hpp"

namespace sparrow_ipc
{
    namespace
    {
        // Integer bit width constants
        constexpr int32_t BIT_WIDTH_8 = 8;
        constexpr int32_t BIT_WIDTH_16 = 16;
        constexpr int32_t BIT_WIDTH_32 = 32;
        constexpr int32_t BIT_WIDTH_64 = 64;

        // End-of-stream marker size in bytes
        constexpr size_t END_OF_STREAM_MARKER_SIZE = 8;
    }

    const org::apache::arrow::flatbuf::RecordBatch*
    deserialize_record_batch_message(std::span<const uint8_t> data, size_t& current_offset)
    {
        current_offset += sizeof(uint32_t);
        const auto message_data = data.subspan(current_offset);
        const auto* batch_message = org::apache::arrow::flatbuf::GetMessage(message_data.data());
        if (batch_message->header_type() != org::apache::arrow::flatbuf::MessageHeader::RecordBatch)
        {
            throw std::runtime_error("Expected RecordBatch message, but got a different type.");
        }
        return static_cast<const org::apache::arrow::flatbuf::RecordBatch*>(batch_message->header());
    }

    /**
     * @brief Deserializes arrays from an Apache Arrow RecordBatch using the provided schema.
     *
     * This function processes each field in the schema and deserializes the corresponding
     * data from the RecordBatch into sparrow::array objects. It handles various Arrow data
     * types including primitive types (bool, integers, floating point), binary data, string
     * data, fixed-size binary data, and interval types.
     *
     * @param record_batch The Apache Arrow FlatBuffer RecordBatch containing the serialized data
     * @param schema The Apache Arrow FlatBuffer Schema defining the structure and types of the data
     * @param encapsulated_message The message containing the binary data buffers
     * @param field_metadata Metadata associated with each field in the schema
     *
     * @return std::vector<sparrow::array> A vector of deserialized arrays, one for each field in the schema
     *
     * @throws std::runtime_error If an unsupported data type, integer bit width, floating point precision,
     *         or interval unit is encountered
     *
     * @note The function maintains a buffer index that is incremented as it processes each field
     *       to correctly map data buffers to their corresponding arrays.
     */
    std::vector<sparrow::array> get_arrays_from_record_batch(
        const org::apache::arrow::flatbuf::RecordBatch& record_batch,
        const org::apache::arrow::flatbuf::Schema& schema,
        const encapsulated_message& encapsulated_message,
        const std::vector<std::optional<std::vector<sparrow::metadata_pair>>>& field_metadata
    )
    {
        size_t buffer_index = 0;

        const size_t num_fields = schema.fields() == nullptr ? 0 : static_cast<size_t>(schema.fields()->size());
        std::vector<sparrow::array> arrays;
        if (num_fields == 0)
        {
            return arrays;
        }
        arrays.reserve(num_fields);
        size_t field_idx = 0;
        for (const auto field : *(schema.fields()))
        {
            const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                fb_custom_metadata = field->custom_metadata();
            const std::optional<std::vector<sparrow::metadata_pair>>& metadata = field_metadata[field_idx++];
            const std::string name = field->name() == nullptr ? "" : field->name()->str();
            const bool nullable = field->nullable();
            const auto field_type = field->type_type();
            // TODO rename all the deserialize_non_owning... fcts since this is not correct anymore
            const auto deserialize_non_owning_primitive_array_lambda = [&]<typename T>()
            {
                return deserialize_non_owning_primitive_array<T>(
                    record_batch,
                    encapsulated_message.body(),
                    name,
                    metadata,
                    nullable,
                    buffer_index
                );
            };
            switch (field_type)
            {
                case org::apache::arrow::flatbuf::Type::Bool:
                    arrays.emplace_back(
                        deserialize_non_owning_primitive_array_lambda.template operator()<bool>()
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::Int:
                {
                    const auto* int_type = field->type_as_Int();
                    const auto bit_width = int_type->bitWidth();
                    const bool is_signed = int_type->is_signed();

                    if (is_signed)
                    {
                        switch (bit_width)
                        {
                                // clang-format off
                                        case BIT_WIDTH_8:  arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int8_t>()); break;
                                        case BIT_WIDTH_16: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int16_t>()); break;
                                        case BIT_WIDTH_32: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int32_t>()); break;
                                        case BIT_WIDTH_64: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<int64_t>()); break;
                                        default: throw std::runtime_error("Unsupported integer bit width: " + std::to_string(bit_width));
                                // clang-format on
                        }
                    }
                    else
                    {
                        switch (bit_width)
                        {
                                // clang-format off
                                        case BIT_WIDTH_8:  arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint8_t>()); break;
                                        case BIT_WIDTH_16: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint16_t>()); break;
                                        case BIT_WIDTH_32: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint32_t>()); break;
                                        case BIT_WIDTH_64: arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<uint64_t>()); break;
                                        default: throw std::runtime_error("Unsupported integer bit width: " + std::to_string(bit_width));
                                // clang-format on
                        }
                    }
                }
                break;
                case org::apache::arrow::flatbuf::Type::FloatingPoint:
                {
                    const auto* float_type = field->type_as_FloatingPoint();
                    switch (float_type->precision())
                    {
                            // clang-format off
                                    case org::apache::arrow::flatbuf::Precision::HALF:
                                        arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<sparrow::float16_t>());
                                        break;
                                    case org::apache::arrow::flatbuf::Precision::SINGLE:
                                        arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<float>());
                                        break;
                                    case org::apache::arrow::flatbuf::Precision::DOUBLE:
                                        arrays.emplace_back(deserialize_non_owning_primitive_array_lambda.template operator()<double>());
                                        break;
                                    default:
                                        throw std::runtime_error(
                                            "Unsupported floating point precision: "
                                            + std::to_string(static_cast<int>(float_type->precision()))
                                        );
                            // clang-format on
                    }
                    break;
                }
                case org::apache::arrow::flatbuf::Type::FixedSizeBinary:
                {
                    const auto* fixed_size_binary_field = field->type_as_FixedSizeBinary();
                    arrays.emplace_back(deserialize_non_owning_fixedwidthbinary(
                        record_batch,
                        encapsulated_message.body(),
                        name,
                        metadata,
                        nullable,
                        buffer_index,
                        fixed_size_binary_field->byteWidth()
                    ));
                    break;
                }
                case org::apache::arrow::flatbuf::Type::Binary:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::binary_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            nullable,
                            buffer_index
                        )
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::LargeBinary:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::big_binary_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            nullable,
                            buffer_index
                        )
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::Utf8:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::string_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            nullable,
                            buffer_index
                        )
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::LargeUtf8:
                    arrays.emplace_back(
                        deserialize_non_owning_variable_size_binary<sparrow::big_string_array>(
                            record_batch,
                            encapsulated_message.body(),
                            name,
                            metadata,
                            nullable,
                            buffer_index
                        )
                    );
                    break;
                case org::apache::arrow::flatbuf::Type::Interval:
                {
                    const auto* interval_type = field->type_as_Interval();
                    const org::apache::arrow::flatbuf::IntervalUnit interval_unit = interval_type->unit();
                    switch (interval_unit)
                    {
                        case org::apache::arrow::flatbuf::IntervalUnit::YEAR_MONTH:
                            arrays.emplace_back(
                                deserialize_non_owning_interval_array<sparrow::chrono::months>(
                                    record_batch,
                                    encapsulated_message.body(),
                                    name,
                                    metadata,
                                    nullable,
                                    buffer_index
                                )
                            );
                            break;
                        case org::apache::arrow::flatbuf::IntervalUnit::DAY_TIME:
                            arrays.emplace_back(
                                deserialize_non_owning_interval_array<sparrow::days_time_interval>(
                                    record_batch,
                                    encapsulated_message.body(),
                                    name,
                                    metadata,
                                    nullable,
                                    buffer_index
                                )
                            );
                            break;
                        case org::apache::arrow::flatbuf::IntervalUnit::MONTH_DAY_NANO:
                            arrays.emplace_back(
                                deserialize_non_owning_interval_array<sparrow::month_day_nanoseconds_interval>(
                                    record_batch,
                                    encapsulated_message.body(),
                                    name,
                                    metadata,
                                    nullable,
                                    buffer_index
                                )
                            );
                            break;
                        default:
                            throw std::runtime_error(
                                "Unsupported interval unit: " + std::to_string(static_cast<int>(interval_unit))
                            );
                    }
                }
                break;
                case org::apache::arrow::flatbuf::Type::Duration:
                {
                    const auto* duration_type = field->type_as_Duration();
                    const org::apache::arrow::flatbuf::TimeUnit time_unit = duration_type->unit();
                    switch (time_unit)
                    {
                        case org::apache::arrow::flatbuf::TimeUnit::SECOND:
                            arrays.emplace_back(
                                deserialize_non_owning_duration_array<std::chrono::seconds>(
                                    record_batch,
                                    encapsulated_message.body(),
                                    name,
                                    metadata,
                                    nullable,
                                    buffer_index
                                )
                            );
                            break;
                        case org::apache::arrow::flatbuf::TimeUnit::MILLISECOND:
                            arrays.emplace_back(
                                deserialize_non_owning_duration_array<std::chrono::milliseconds>(
                                    record_batch,
                                    encapsulated_message.body(),
                                    name,
                                    metadata,
                                    nullable,
                                    buffer_index
                                )
                            );
                            break;
                        case org::apache::arrow::flatbuf::TimeUnit::MICROSECOND:
                            arrays.emplace_back(
                                deserialize_non_owning_duration_array<std::chrono::microseconds>(
                                    record_batch,
                                    encapsulated_message.body(),
                                    name,
                                    metadata,
                                    nullable,
                                    buffer_index
                                )
                            );
                            break;
                        case org::apache::arrow::flatbuf::TimeUnit::NANOSECOND:
                            arrays.emplace_back(
                                deserialize_non_owning_duration_array<std::chrono::nanoseconds>(
                                    record_batch,
                                    encapsulated_message.body(),
                                    name,
                                    metadata,
                                    nullable,
                                    buffer_index
                                )
                            );
                            break;
                        default:
                            throw std::runtime_error(
                                "Unsupported duration time unit: "
                                + std::to_string(static_cast<int>(time_unit))
                            );
                    }
                }
                break;
                case org::apache::arrow::flatbuf::Type::Null:
                    arrays.emplace_back(deserialize_non_owning_null(
                        record_batch,
                        encapsulated_message.body(),
                        name,
                        metadata,
                        nullable,
                        buffer_index
                    ));
                    break;
                case org::apache::arrow::flatbuf::Type::Decimal:
                {
                    const auto decimal_field = field->type_as_Decimal();
                    const auto scale = decimal_field->scale();
                    const auto precision = decimal_field->precision();
                    if (decimal_field->bitWidth() == 32)
                    {
                        arrays.emplace_back(
                            deserialize_non_owning_decimal<sparrow::decimal<int32_t>>(
                                record_batch,
                                encapsulated_message.body(),
                                name,
                                metadata,
                                nullable,
                                buffer_index,
                                scale,
                                precision
                            )
                        );
                    }
                    else if (decimal_field->bitWidth() == 64)
                    {
                        arrays.emplace_back(
                            deserialize_non_owning_decimal<sparrow::decimal<int64_t>>(
                                record_batch,
                                encapsulated_message.body(),
                                name,
                                metadata,
                                nullable,
                                buffer_index,
                                scale,
                                precision
                            )
                        );
                    }
                    else if (decimal_field->bitWidth() == 128)
                    {
                        arrays.emplace_back(
                            deserialize_non_owning_decimal<sparrow::decimal<sparrow::int128_t>>(
                                record_batch,
                                encapsulated_message.body(),
                                name,
                                metadata,
                                nullable,
                                buffer_index,
                                scale,
                                precision
                            )
                        );
                    }
                    else if (decimal_field->bitWidth() == 256)
                    {
                        arrays.emplace_back(
                            deserialize_non_owning_decimal<sparrow::decimal<sparrow::int256_t>>(
                                record_batch,
                                encapsulated_message.body(),
                                name,
                                metadata,
                                nullable,
                                buffer_index,
                                scale,
                                precision
                            )
                        );
                    }
                    break;
                }
                default:
                    throw std::runtime_error(
                        "Unsupported field type: " + std::to_string(static_cast<int>(field_type))
                        + " for field '" + name + "'"
                    );
            }
        }
        return arrays;
    }

    std::vector<sparrow::record_batch> deserialize_stream(std::span<const uint8_t> data)
    {
        const org::apache::arrow::flatbuf::Schema* schema = nullptr;
        std::vector<sparrow::record_batch> record_batches;
        std::vector<std::string> field_names;
        std::vector<bool> fields_nullable;
        std::vector<sparrow::data_type> field_types;
        std::vector<std::optional<std::vector<sparrow::metadata_pair>>> fields_metadata;

        while (!data.empty())
        {
            // Check for end-of-stream marker
            if (data.size() >= END_OF_STREAM_MARKER_SIZE
                && is_end_of_stream(data.subspan(0, END_OF_STREAM_MARKER_SIZE)))
            {
                break;
            }

            const auto [encapsulated_message, rest] = extract_encapsulated_message(data);
            const org::apache::arrow::flatbuf::Message* message = encapsulated_message.flat_buffer_message();

            if (message == nullptr)
            {
                throw std::invalid_argument("Extracted flatbuffers message is null.");
            }

            switch (message->header_type())
            {
                case org::apache::arrow::flatbuf::MessageHeader::Schema:
                {
                    schema = message->header_as_Schema();
                    const size_t size = schema->fields() == nullptr
                                            ? 0
                                            : static_cast<size_t>(schema->fields()->size());
                    field_names.reserve(size);
                    fields_nullable.reserve(size);
                    fields_metadata.reserve(size);
                    if (schema->fields() == nullptr)
                    {
                        break;
                    }
                    for (const auto field : *(schema->fields()))
                    {
                        if (field != nullptr && field->name() != nullptr)
                        {
                            field_names.emplace_back(field->name()->str());
                        }
                        else
                        {
                            field_names.emplace_back("_unnamed_");
                        }
                        fields_nullable.push_back(field->nullable());
                        const ::flatbuffers::Vector<::flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>>*
                            fb_custom_metadata = field->custom_metadata();
                        std::optional<std::vector<sparrow::metadata_pair>>
                            metadata = fb_custom_metadata == nullptr
                                           ? std::nullopt
                                           : std::make_optional(to_sparrow_metadata(*fb_custom_metadata));
                        fields_metadata.push_back(std::move(metadata));
                    }
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::RecordBatch:
                {
                    if (schema == nullptr)
                    {
                        throw std::runtime_error("RecordBatch encountered before Schema message.");
                    }
                    const auto* record_batch = message->header_as_RecordBatch();
                    if (record_batch == nullptr)
                    {
                        throw std::runtime_error("RecordBatch message header is null.");
                    }
                    std::vector<sparrow::array> arrays = get_arrays_from_record_batch(
                        *record_batch,
                        *schema,
                        encapsulated_message,
                        fields_metadata
                    );
                    auto names_copy = field_names;
                    sparrow::record_batch sp_record_batch(std::move(names_copy), std::move(arrays));
                    record_batches.emplace_back(std::move(sp_record_batch));
                }
                break;
                case org::apache::arrow::flatbuf::MessageHeader::Tensor:
                case org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch:
                case org::apache::arrow::flatbuf::MessageHeader::SparseTensor:
                    throw std::runtime_error(
                        "Unsupported message type: Tensor, DictionaryBatch, or SparseTensor"
                    );
                default:
                    throw std::runtime_error("Unknown message header type.");
            }
            data = rest;
        }
        return record_batches;
    }
}
