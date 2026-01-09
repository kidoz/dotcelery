# DotCelery Justfile
# Run commands with: just <command>

# Default recipe - show available commands
default:
    @just --list

# Format code using CSharpier
format:
    dotnet tool run csharpier format .

# Check formatting without making changes
format-check:
    dotnet tool run csharpier check .

# Build the solution
build:
    dotnet build

# Build in release mode
build-release:
    dotnet build -c Release

# Run all unit tests
test:
    dotnet test tests/DotCelery.Tests.Unit

# Run all tests (unit + integration)
test-all:
    dotnet test

# Run tests with verbose output
test-verbose:
    dotnet test tests/DotCelery.Tests.Unit --logger "console;verbosity=detailed"

# Clean build artifacts
clean:
    dotnet clean

# Restore dependencies
restore:
    dotnet restore

# Install CSharpier as a local tool
install-tools:
    dotnet tool restore || dotnet tool install csharpier

# Run the demo project
run-demo:
    dotnet run --project samples/DotCelery.Demo

# Pack NuGet packages
pack:
    dotnet pack -c Release

# Format and build
fb: format build

# Format, build, and test
fbt: format build test

# Run benchmarks
bench:
    dotnet run --project benchmarks/DotCelery.Benchmarks -c Release

# Run specific benchmark class
bench-filter FILTER:
    dotnet run --project benchmarks/DotCelery.Benchmarks -c Release -- --filter "*{{FILTER}}*"

# Run benchmarks with memory diagnoser
bench-memory:
    dotnet run --project benchmarks/DotCelery.Benchmarks -c Release -- --memory

# Run benchmarks with shorter iterations (for quick testing)
bench-quick:
    dotnet run --project benchmarks/DotCelery.Benchmarks -c Release -- --job short
