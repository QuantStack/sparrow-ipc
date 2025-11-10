#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <sparrow/record_batch.hpp>

#include "sparrow/json_reader/json_parser.hpp"

#include "doctest/doctest.h"
#include "sparrow_ipc/deserialize.hpp"

// Helper function to execute a command and capture output
struct CommandResult
{
    int exit_code;
    std::string stdout_data;
    std::string stderr_data;
};

#ifdef _WIN32
#include <windows.h>

CommandResult execute_command(const std::string& command)
{
    CommandResult result;
    
    // Create temporary files for stdout and stderr
    const std::string stdout_file = std::tmpnam(nullptr);
    const std::string stderr_file = std::tmpnam(nullptr);
    
    const std::string full_command = command + " > " + stdout_file + " 2> " + stderr_file;
    
    result.exit_code = std::system(full_command.c_str());
    
    // Read stdout
    std::ifstream stdout_stream(stdout_file, std::ios::binary);
    if (stdout_stream)
    {
        std::ostringstream ss;
        ss << stdout_stream.rdbuf();
        result.stdout_data = ss.str();
    }
    
    // Read stderr
    std::ifstream stderr_stream(stderr_file, std::ios::binary);
    if (stderr_stream)
    {
        std::ostringstream ss;
        ss << stderr_stream.rdbuf();
        result.stderr_data = ss.str();
    }
    
    // Clean up
    std::filesystem::remove(stdout_file);
    std::filesystem::remove(stderr_file);
    
    return result;
}

#else
#include <array>
#include <memory>

CommandResult execute_command(const std::string& command)
{
    CommandResult result;
    
    // Check if command already contains output redirection
    const bool has_redirection = (command.find('>') != std::string::npos);
    
    if (has_redirection)
    {
        // Command already has redirection, execute as-is
        // But we still want to capture stderr for error checking
        const std::filesystem::path stderr_file = std::filesystem::temp_directory_path() / ("stderr_" + std::to_string(std::time(nullptr)));
        const std::string full_command = command + " 2> " + stderr_file.string();
        result.exit_code = std::system(full_command.c_str());
        
        // Read stderr
        std::ifstream stderr_stream(stderr_file, std::ios::binary);
        if (stderr_stream)
        {
            std::ostringstream ss;
            ss << stderr_stream.rdbuf();
            result.stderr_data = ss.str();
        }
        
        // Clean up
        std::filesystem::remove(stderr_file);
    }
    else
    {
        // Create temporary files for stdout and stderr
        const std::filesystem::path stdout_file = std::filesystem::temp_directory_path() / ("stdout_" + std::to_string(std::time(nullptr)));
        const std::filesystem::path stderr_file = std::filesystem::temp_directory_path() / ("stderr_" + std::to_string(std::time(nullptr)));
        
        // The command string is already properly formed (executable path + args)
        // We need to redirect stdout and stderr to files
        const std::string full_command = command + " > " + stdout_file.string() + " 2> " + stderr_file.string();
        
        result.exit_code = std::system(full_command.c_str());
    
        // Read stdout
        std::ifstream stdout_stream(stdout_file, std::ios::binary);
        if (stdout_stream)
        {
            std::ostringstream ss;
            ss << stdout_stream.rdbuf();
            result.stdout_data = ss.str();
        }
        
        // Read stderr
        std::ifstream stderr_stream(stderr_file, std::ios::binary);
        if (stderr_stream)
        {
            std::ostringstream ss;
            ss << stderr_stream.rdbuf();
            result.stderr_data = ss.str();
        }
        
        // Clean up
        std::filesystem::remove(stdout_file);
        std::filesystem::remove(stderr_file);
    }
    
    return result;
}
#endif

// Helper to compare record batches
void compare_record_batches(
    const std::vector<sparrow::record_batch>& record_batches_1,
    const std::vector<sparrow::record_batch>& record_batches_2
)
{
    REQUIRE_EQ(record_batches_1.size(), record_batches_2.size());
    for (size_t i = 0; i < record_batches_1.size(); ++i)
    {
        REQUIRE_EQ(record_batches_1[i].nb_columns(), record_batches_2[i].nb_columns());
        for (size_t y = 0; y < record_batches_1[i].nb_columns(); y++)
        {
            const auto& column_1 = record_batches_1[i].get_column(y);
            const auto& column_2 = record_batches_2[i].get_column(y);
            REQUIRE_EQ(column_1.size(), column_2.size());
            CHECK_EQ(record_batches_1[i].names()[y], record_batches_2[i].names()[y]);
            for (size_t z = 0; z < column_1.size(); z++)
            {
                const auto col_name = column_1.name().value_or("NA");
                INFO("Comparing batch " << i << ", column " << y << " named: " << col_name << ", row " << z);
                REQUIRE_EQ(column_1.data_type(), column_2.data_type());
                CHECK_EQ(column_1[z], column_2[z]);
            }
        }
    }
}

TEST_SUITE("Integration Tools Tests")
{
    // Get paths to test data
    const std::filesystem::path arrow_testing_data_dir = ARROW_TESTING_DATA_DIR;
    const std::filesystem::path tests_resources_files_path = 
        arrow_testing_data_dir / "data" / "arrow-ipc-stream" / "integration" / "cpp-21.0.0";
    
    // Paths to the executables - defined at compile time
    const std::filesystem::path exe_dir = INTEGRATION_TOOLS_DIR;
    const std::filesystem::path file_to_stream_exe = exe_dir / "file_to_stream";
    const std::filesystem::path stream_to_file_exe = exe_dir / "stream_to_file";
    
    // Helper to build command with properly quoted executable
    auto make_command = [](const std::filesystem::path& exe, const std::string& args = "") {
        std::string cmd = "\"" + exe.string() + "\"";
        if (!args.empty()) {
            cmd += " " + args;
        }
        return cmd;
    };

    TEST_CASE("file_to_stream - No arguments")
    {
        auto result = execute_command(make_command(file_to_stream_exe));
        CHECK_NE(result.exit_code, 0);
        CHECK(result.stderr_data.find("Usage:") != std::string::npos);
    }

    TEST_CASE("file_to_stream - Non-existent file")
    {
        const std::string non_existent = "non_existent_file_12345.json";
        auto result = execute_command(make_command(file_to_stream_exe, non_existent));
        CHECK_NE(result.exit_code, 0);
        CHECK(result.stderr_data.find("not found") != std::string::npos);
    }

    TEST_CASE("stream_to_file - No arguments")
    {
        auto result = execute_command(make_command(stream_to_file_exe));
        CHECK_NE(result.exit_code, 0);
        CHECK(result.stderr_data.find("Usage:") != std::string::npos);
    }

    TEST_CASE("stream_to_file - Only one argument")
    {
        auto result = execute_command(make_command(stream_to_file_exe, "output.stream"));
        CHECK_NE(result.exit_code, 0);
        CHECK(result.stderr_data.find("Usage:") != std::string::npos);
    }

    TEST_CASE("stream_to_file - Non-existent input file")
    {
        const std::string non_existent = "non_existent_file_12345.stream";
        const std::string output_file = "output.stream";
        auto result = execute_command(make_command(stream_to_file_exe, non_existent + " " + output_file));
        CHECK_NE(result.exit_code, 0);
        CHECK(result.stderr_data.find("not found") != std::string::npos);
    }

    TEST_CASE("file_to_stream - Convert JSON to stream")
    {
        // Test with a known good JSON file
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";
        
        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        const std::filesystem::path output_stream = std::filesystem::temp_directory_path() / "test_output.stream";
        
        // Execute file_to_stream
        const std::string command = "\"" + file_to_stream_exe.string() + "\" \"" + json_file.string() + "\" > \"" + output_stream.string() + "\"";
        auto result = execute_command(command);
        
        CHECK_EQ(result.exit_code, 0);
        CHECK(std::filesystem::exists(output_stream));
        CHECK_GT(std::filesystem::file_size(output_stream), 0);

        // Verify the output is a valid stream by deserializing it
        std::ifstream stream_file(output_stream, std::ios::binary);
        REQUIRE(stream_file.is_open());
        
        std::vector<uint8_t> stream_data(
            (std::istreambuf_iterator<char>(stream_file)),
            std::istreambuf_iterator<char>()
        );
        stream_file.close();

        // Should be able to deserialize without errors
        CHECK_NOTHROW(sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data)));

        // Clean up
        std::filesystem::remove(output_stream);
    }

    TEST_CASE("stream_to_file - Process stream file")
    {
        const std::filesystem::path input_stream = tests_resources_files_path / "generated_primitive.stream";
        
        if (!std::filesystem::exists(input_stream))
        {
            MESSAGE("Skipping test: test file not found at " << input_stream);
            return;
        }

        const std::filesystem::path output_stream = std::filesystem::temp_directory_path() / "test_stream_output.stream";
        
        // Execute stream_to_file
        const std::string command = "\"" + stream_to_file_exe.string() + "\" \"" + input_stream.string() + "\" \"" + output_stream.string() + "\"";
        auto result = execute_command(command);
        
        CHECK_EQ(result.exit_code, 0);
        CHECK(std::filesystem::exists(output_stream));
        CHECK_GT(std::filesystem::file_size(output_stream), 0);

        // Verify the output is a valid stream
        std::ifstream output_file(output_stream, std::ios::binary);
        REQUIRE(output_file.is_open());
        
        std::vector<uint8_t> output_data(
            (std::istreambuf_iterator<char>(output_file)),
            std::istreambuf_iterator<char>()
        );
        output_file.close();

        CHECK_NOTHROW(sparrow_ipc::deserialize_stream(std::span<const uint8_t>(output_data)));

        // Clean up
        std::filesystem::remove(output_stream);
    }

    TEST_CASE("Round-trip: JSON -> stream -> file -> deserialize")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";
        
        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        const std::filesystem::path intermediate_stream = std::filesystem::temp_directory_path() / "intermediate.stream";
        const std::filesystem::path final_stream = std::filesystem::temp_directory_path() / "final.stream";

        // Step 1: JSON -> stream
        {
            const std::string command = "\"" + file_to_stream_exe.string() + "\" \"" + json_file.string() + "\" > \"" + intermediate_stream.string() + "\"";
            auto result = execute_command(command);
            REQUIRE_EQ(result.exit_code, 0);
            REQUIRE(std::filesystem::exists(intermediate_stream));
        }

        // Step 2: stream -> file
        {
            const std::string command = "\"" + stream_to_file_exe.string() + "\" \"" + intermediate_stream.string() + "\" \"" + final_stream.string() + "\"";
            auto result = execute_command(command);
            REQUIRE_EQ(result.exit_code, 0);
            REQUIRE(std::filesystem::exists(final_stream));
        }

        // Step 3: Compare the results
        // Load original JSON data
        std::ifstream json_input(json_file);
        REQUIRE(json_input.is_open());
        nlohmann::json json_data = nlohmann::json::parse(json_input);
        json_input.close();

        const size_t num_batches = json_data["batches"].size();
        std::vector<sparrow::record_batch> original_batches;
        for (size_t i = 0; i < num_batches; ++i)
        {
            original_batches.emplace_back(
                sparrow::json_reader::build_record_batch_from_json(json_data, i)
            );
        }

        // Load final stream
        std::ifstream final_file(final_stream, std::ios::binary);
        REQUIRE(final_file.is_open());
        std::vector<uint8_t> final_data(
            (std::istreambuf_iterator<char>(final_file)),
            std::istreambuf_iterator<char>()
        );
        final_file.close();

        auto final_batches = sparrow_ipc::deserialize_stream(std::span<const uint8_t>(final_data));

        // Compare
        compare_record_batches(original_batches, final_batches);

        // Clean up
        std::filesystem::remove(intermediate_stream);
        std::filesystem::remove(final_stream);
    }

    TEST_CASE("Paths with spaces")
    {
        const std::filesystem::path json_file = tests_resources_files_path / "generated_primitive.json";
        
        if (!std::filesystem::exists(json_file))
        {
            MESSAGE("Skipping test: test file not found at " << json_file);
            return;
        }

        // Create temporary directory with spaces in the name
        const std::filesystem::path temp_dir = std::filesystem::temp_directory_path() / "test dir with spaces";
        std::filesystem::create_directories(temp_dir);

        const std::filesystem::path output_stream = temp_dir / "output file.stream";
        const std::filesystem::path final_stream = temp_dir / "final output.stream";

        // Step 1: JSON -> stream with spaces in output path
        {
            const std::string command = "\"" + file_to_stream_exe.string() + "\" \"" + json_file.string() + "\" > \"" + output_stream.string() + "\"";
            auto result = execute_command(command);
            CHECK_EQ(result.exit_code, 0);
            CHECK(std::filesystem::exists(output_stream));
        }

        // Step 2: stream -> file with spaces in both paths
        {
            const std::string command = "\"" + stream_to_file_exe.string() + "\" \"" + output_stream.string() + "\" \"" + final_stream.string() + "\"";
            auto result = execute_command(command);
            CHECK_EQ(result.exit_code, 0);
            CHECK(std::filesystem::exists(final_stream));
        }

        // Verify the final output is valid
        std::ifstream final_file(final_stream, std::ios::binary);
        REQUIRE(final_file.is_open());
        std::vector<uint8_t> final_data(
            (std::istreambuf_iterator<char>(final_file)),
            std::istreambuf_iterator<char>()
        );
        final_file.close();

        CHECK_NOTHROW(sparrow_ipc::deserialize_stream(std::span<const uint8_t>(final_data)));

        // Clean up
        std::filesystem::remove_all(temp_dir);
    }

    TEST_CASE("Multiple test files")
    {
        const std::vector<std::string> test_files = {
            "generated_primitive",
            "generated_binary",
            "generated_primitive_zerolength",
            "generated_binary_zerolength"
        };

        for (const auto& test_file : test_files)
        {
            const std::filesystem::path json_file = tests_resources_files_path / (test_file + ".json");
            
            if (!std::filesystem::exists(json_file))
            {
                MESSAGE("Skipping test file: " << json_file);
                continue;
            }

            SUBCASE(test_file.c_str())
            {
                const std::filesystem::path output_stream = std::filesystem::temp_directory_path() / (test_file + "_output.stream");

                // Convert JSON to stream
                const std::string command = "\"" + file_to_stream_exe.string() + "\" \"" + json_file.string() + "\" > \"" + output_stream.string() + "\"";
                auto result = execute_command(command);
                
                CHECK_EQ(result.exit_code, 0);
                CHECK(std::filesystem::exists(output_stream));

                // Deserialize and verify
                std::ifstream stream_file(output_stream, std::ios::binary);
                if (stream_file.is_open())
                {
                    std::vector<uint8_t> stream_data(
                        (std::istreambuf_iterator<char>(stream_file)),
                        std::istreambuf_iterator<char>()
                    );
                    stream_file.close();

                    CHECK_NOTHROW(sparrow_ipc::deserialize_stream(std::span<const uint8_t>(stream_data)));
                }

                // Clean up
                std::filesystem::remove(output_stream);
            }
        }
    }
}
