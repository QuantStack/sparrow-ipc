# Development build                             {#dev_build}

Here we describe how to build the project for development purposes on **Linux** or **macOS**.
For **Windows**, the instructions are similar.

List of CMake options:
- `ACTIVATE_LINTER`: Create targets to run clang-format and clang-tidy (default: OFF)
- `ACTIVATE_LINTER_DURING_COMPILATION`: Run linter during the compilation (default: OFF),
  requires `ACTIVATE_LINTER` to be ON
- `SPARROW_IPC_BUILD_DOCS`: Build the documentation (default: OFF)
- `SPARROW_IPC_BUILD_TESTS`: Build the tests (default: OFF)
- `SPARROW_IPC_BUILD_SHARED`: Build sparrow-ipc as a shared library (default: ON)
- `SPARROW_IPC_ENABLE_COVERAGE`: Enable coverage reporting (default: OFF)

## Building

### ... with mamba/micromamba

First, we create a conda environment with all required development dependencies:

```bash
mamba env create -f environment-dev.yml
```

Then we activate the environment:

```bash
mamba activate sparrow-ipc
```

Create a build directory and run cmake from it:

```bash
mkdir build
cd build
cmake .. \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
    -DCMAKE_PREFIX_PATH=$CONDA_PREFIX
```

And finally, build the project:

```bash
make -j12
```


### ... with Conan

If you prefer to use Conan, you can follow these steps:
First, install the required dependencies using Conan:

```bash
conan install .. --build=missing -s:a compiler.cppstd=20 -o:a build_tests=True
```
Available Conan options:
- `build_tests`: Build the tests (default: False)
- `generate_documentation`: Generate documentation (default: False)

Then, run the cmake configuration:

```bash
mkdir build
cd build
cmake --preset conan-release
```

## Running the tests

To run the tests, the easy way is to use the cmake targets:
- `run_tests`: Runs all tests without JUnit report
- `run_tests_with_junit_report`: Runs all tests and generates a JUnit report in the build directory

```bash
cmake --build . --config [Release/Debug/...] --target [TARGET_NAME]
```

## Building the documentation

To build the documentation, you can use the `docs` target:
```bash
cmake --build . --target docs
```

The documentation will be located in the `docs/html` folder in the build directory. You can open `docs/html/index.html` in your browser to view it.
