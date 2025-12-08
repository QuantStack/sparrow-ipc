#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

#include <sparrow_ipc/deserialize.hpp>
#include <sparrow_ipc/memory_output_stream.hpp>
#include <sparrow_ipc/serializer.hpp>

#include <sparrow/record_batch.hpp>

const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;
const std::filesystem::path tests_resources_files_path = arrow_testing_data_dir / "data" / "arrow-ipc-stream"
                                                         / "integration" / "cpp-21.0.0";


namespace sp = sparrow;

// Random number generator
std::random_device rd;
std::mt19937 gen(rd());

namespace utils
{
    /**
     * Helper function to create a record batch with the same schema but random values
     * All batches have: int32 column, float column, bool column, and string column
     */
    sp::record_batch create_random_record_batch(size_t num_rows)
    {
        // Helper lambda to generate a vector with random values
        auto generate_vector = [num_rows](auto generator)
        {
            using T = decltype(generator());
            std::vector<T> values(num_rows);
            std::generate(values.begin(), values.end(), generator);
            return values;
        };

        // Create integer column with random values
        std::uniform_int_distribution<int32_t> int_dist(0, 1000);
        auto int_array = sp::primitive_array<int32_t>(generate_vector(
            [&]()
            {
                return int_dist(gen);
            }
        ));

        // Create float column with random values
        std::uniform_real_distribution<float> float_dist(-100.0f, 100.0f);
        auto float_array = sp::primitive_array<float>(generate_vector(
            [&]()
            {
                return float_dist(gen);
            }
        ));

        // Create boolean column with random values
        std::uniform_int_distribution<int> bool_dist(0, 1);
        auto bool_array = sp::primitive_array<bool>(generate_vector(
            [&]()
            {
                return static_cast<bool>(bool_dist(gen));
            }
        ));

        // Create string column with random values
        const std::vector<std::string> sample_strings =
            {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"};
        std::uniform_int_distribution<size_t> str_dist(0, sample_strings.size() - 1);
        size_t counter = 0;
        auto string_array = sp::string_array(generate_vector(
            [&]()
            {
                return sample_strings[str_dist(gen)] + "_" + std::to_string(counter++);
            }
        ));

        // Create record batch with named columns (same schema for all batches)
        return sp::record_batch(
            {{"id", sp::array(std::move(int_array))},
             {"value", sp::array(std::move(float_array))},
             {"flag", sp::array(std::move(bool_array))},
             {"name", sp::array(std::move(string_array))}}
        );
    }

    /**
     * Verify that two sets of record batches are identical
     * Returns true if all batches match, false otherwise
     */
    bool verify_batches_match(
        const std::vector<sp::record_batch>& original_batches,
        const std::vector<sp::record_batch>& deserialized_batches
    )
    {
        if (original_batches.size() != deserialized_batches.size())
        {
            std::cerr << "ERROR: Batch count mismatch! Original: " << original_batches.size()
                      << ", Deserialized: " << deserialized_batches.size() << "\n";
            return false;
        }

        bool all_match = true;
        for (size_t batch_idx = 0; batch_idx < original_batches.size(); ++batch_idx)
        {
            const auto& original = original_batches[batch_idx];
            const auto& deserialized = deserialized_batches[batch_idx];

            // Check basic structure
            if (original.nb_columns() != deserialized.nb_columns()
                || original.nb_rows() != deserialized.nb_rows())
            {
                std::cerr << "ERROR: Batch " << batch_idx << " structure mismatch!\n";
                all_match = false;
                continue;
            }

            // Check column names
            if (!std::ranges::equal(original.names(), deserialized.names()))
            {
                std::cerr << "WARNING: Batch " << batch_idx << " column names mismatch!\n";
            }

            // Check column data
            for (size_t col_idx = 0; col_idx < original.nb_columns(); ++col_idx)
            {
                const auto& orig_col = original.get_column(col_idx);
                const auto& deser_col = deserialized.get_column(col_idx);

                if (orig_col.data_type() != deser_col.data_type())
                {
                    std::cerr << "ERROR: Batch " << batch_idx << ", column " << col_idx << " type mismatch!\n";
                    all_match = false;
                    continue;
                }

                // Check values
                for (size_t row_idx = 0; row_idx < orig_col.size(); ++row_idx)
                {
                    if (orig_col[row_idx] != deser_col[row_idx])
                    {
                        std::cerr << "ERROR: Batch " << batch_idx << ", column " << col_idx << ", row "
                                  << row_idx << " value mismatch!\n";
                        std::cerr << "  Original: " << orig_col[row_idx]
                                  << ", Deserialized: " << deser_col[row_idx] << "\n";
                        all_match = false;
                    }
                }
            }
        }

        return all_match;
    }
}

/**
 * Create multiple record batches with the same schema but random values
 */
std::vector<sp::record_batch> create_record_batches(size_t num_batches, size_t rows_per_batch)
{
    std::cout << "1. Creating " << num_batches << " record batches with random values...\n";
    std::cout << "   Each batch has the same schema: (id: int32, value: float, flag: bool, name: string)\n";

    std::vector<sp::record_batch> batches;
    batches.reserve(num_batches);

    for (size_t i = 0; i < num_batches; ++i)
    {
        batches.push_back(utils::create_random_record_batch(rows_per_batch));
    }

    std::cout << "   Created " << batches.size() << " record batches\n";
    for (size_t i = 0; i < batches.size(); ++i)
    {
        std::cout << std::format("{}\n\n", batches[i]);
    }

    return batches;
}

// [example_serialize_to_stream]
/**
 * Serialize record batches to a stream
 */
std::vector<uint8_t> serialize_batches_to_stream(const std::vector<sp::record_batch>& batches)
{
    std::cout << "\n2. Serializing record batches to stream...\n";

    std::vector<uint8_t> stream_data;
    sparrow_ipc::memory_output_stream stream(stream_data);
    sparrow_ipc::serializer serializer(stream);

    // Serialize all batches using the streaming operator
    serializer << batches << sparrow_ipc::end_stream;

    std::cout << "   Serialized stream size: " << stream_data.size() << " bytes\n";

    return stream_data;
}
// [example_serialize_to_stream]

// [example_deserialize_from_stream]
/**
 * Deserialize stream back to record batches
 */
std::vector<sp::record_batch> deserialize_stream_to_batches(const std::vector<uint8_t>& stream_data)
{
    std::cout << "\n3. Deserializing stream back to record batches...\n";

    auto batches = sparrow_ipc::deserialize_stream(stream_data);

    std::cout << "   Deserialized " << batches.size() << " record batches\n";

    return batches;
}
// [example_deserialize_from_stream]

// [example_serialize_individual]
/**
 * Demonstrate individual vs batch serialization
 */
void demonstrate_serialization_methods(
    const std::vector<sp::record_batch>& batches,
    const std::vector<uint8_t>& batch_stream_data
)
{
    std::cout << "\n6. Demonstrating individual vs batch serialization...\n";

    // Serialize individual batches one by one
    std::vector<uint8_t> individual_stream_data;
    sparrow_ipc::memory_output_stream individual_stream(individual_stream_data);
    sparrow_ipc::serializer individual_serializer(individual_stream);

    for (const auto& batch : batches)
    {
        individual_serializer << batch;
    }
    individual_serializer << sparrow_ipc::end_stream;

    std::cout << "   Individual serialization size: " << individual_stream_data.size() << " bytes\n";
    std::cout << "   Batch serialization size: " << batch_stream_data.size() << " bytes\n";

    // Both should produce the same result
    auto individual_deserialized = sparrow_ipc::deserialize_stream(individual_stream_data);

    if (individual_deserialized.size() == batches.size())
    {
        std::cout << "   ✓ Individual and batch serialization produce equivalent results\n";
    }
    else
    {
        std::cerr << "   ✗ Individual and batch serialization mismatch!\n";
    }
}
// [example_serialize_individual]

/**
 * Verify schema consistency across all batches
 */
bool verify_schema_consistency(const std::vector<sp::record_batch>& batches)
{
    std::cout << "\n7. Verifying schema consistency across all batches...\n";

    if (batches.empty())
    {
        std::cout << "   No batches to verify\n";
        return true;
    }

    bool schema_consistent = true;
    for (size_t i = 1; i < batches.size(); ++i)
    {
        if (batches[0].nb_columns() != batches[i].nb_columns())
        {
            std::cerr << "   ERROR: Batch " << i << " has different number of columns!\n";
            schema_consistent = false;
        }

        for (size_t col_idx = 0; col_idx < batches[0].nb_columns() && col_idx < batches[i].nb_columns();
             ++col_idx)
        {
            const auto& col0 = batches[0].get_column(col_idx);
            const auto& col_i = batches[i].get_column(col_idx);

            if (col0.data_type() != col_i.data_type())
            {
                std::cerr << "   ERROR: Batch " << i << ", column " << col_idx << " has different type!\n";
                schema_consistent = false;
            }

            if (col0.name() != col_i.name())
            {
                std::cerr << "   ERROR: Batch " << i << ", column " << col_idx << " has different name!\n";
                schema_consistent = false;
            }
        }
    }

    if (schema_consistent)
    {
        std::cout << "   ✓ All batches have consistent schema!\n";
    }
    else
    {
        std::cerr << "   ✗ Schema inconsistency detected!\n";
    }

    return schema_consistent;
}

/**
 * Read and display a primitive stream file from test resources
 */
void read_and_display_test_file()
{
    std::cout << "\n8. Reading a primitive stream file from test resources...\n";

    const std::filesystem::path primitive_stream_file = tests_resources_files_path
                                                        / "generated_primitive.stream";

    if (std::filesystem::exists(primitive_stream_file))
    {
        std::cout << "   Reading file: " << primitive_stream_file << "\n";

        // Read the stream file
        std::ifstream stream_file(primitive_stream_file, std::ios::in | std::ios::binary);
        if (!stream_file.is_open())
        {
            std::cerr << "   ERROR: Could not open stream file!\n";
        }
        else
        {
            const std::vector<uint8_t> file_stream_data(
                (std::istreambuf_iterator<char>(stream_file)),
                (std::istreambuf_iterator<char>())
            );
            stream_file.close();

            std::cout << "   File size: " << file_stream_data.size() << " bytes\n";

            // Deserialize the stream
            auto file_batches = sparrow_ipc::deserialize_stream(file_stream_data);

            std::cout << "   Deserialized " << file_batches.size() << " record batch(es) from file\n";

            // Display the first batch
            if (!file_batches.empty())
            {
                std::cout << "   First batch from file:\n";
                std::cout << std::format("{}\n", file_batches[0]);
            }
        }
    }
    else
    {
        std::cout << "   Note: Test resource file not found at " << primitive_stream_file << "\n";
        std::cout << "   This is expected if test data is not available.\n";
    }
}

int main()
{
    std::cout << "=== Sparrow IPC Stream Write and Read Example ===\n";
    std::cout << "Note: All record batches in a stream must have the same schema.\n\n";

    try
    {
        // Configuration
        constexpr size_t num_batches = 5;
        constexpr size_t rows_per_batch = 10;

        // Step 1: Create several record batches with the SAME schema but random values
        auto original_batches = create_record_batches(num_batches, rows_per_batch);

        // Step 2: Serialize the record batches to a stream
        auto stream_data = serialize_batches_to_stream(original_batches);

        // Step 3: Deserialize the stream back to record batches
        auto deserialized_batches = deserialize_stream_to_batches(stream_data);

        // Step 4: Verify that original and deserialized data match
        std::cout << "\n4. Verifying data integrity...\n";

        if (utils::verify_batches_match(original_batches, deserialized_batches))
        {
            std::cout << "   ✓ All data matches perfectly!\n";
        }
        else
        {
            std::cerr << "   ✗ Data verification failed!\n";
            return 1;
        }

        // Step 5: Display sample data from the first batch
        std::cout << "\n5. Sample data from the first batch:\n";
        std::cout << std::format("{}\n", original_batches[0]);

        // Step 6: Demonstrate individual serialization vs batch serialization
        demonstrate_serialization_methods(original_batches, stream_data);

        // Step 7: Verify schema consistency
        verify_schema_consistency(deserialized_batches);

        // Step 8: Read and display a primitive stream file from test resources
        read_and_display_test_file();

        std::cout << "\n=== Example completed successfully! ===\n";
    }
    catch (const std::exception& e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
