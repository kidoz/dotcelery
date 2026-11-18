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

# Declare the current public API of every shipped library in PublicAPI.Unshipped.txt
api-format:
    #!/usr/bin/env bash
    set -euo pipefail
    for project in src/*/*.csproj; do
        dotnet format analyzers "$project" --diagnostics RS0016
    done

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
run-demo FRAMEWORK="net11.0":
    dotnet run --project samples/DotCelery.Demo --framework {{FRAMEWORK}}

# Run the canvas workflow example
run-canvas-example FRAMEWORK="net11.0":
    dotnet run --project samples/DotCelery.CanvasExample --framework {{FRAMEWORK}}

# Publish the Native AOT smoke test for the host platform and run it
aot-smoke:
    ./eng/run-aot-smoke.sh

# Start local dependencies used by the Redis/Postgres example
compose-up:
    docker compose up -d redis postgres

# Stop local example dependencies
compose-down:
    docker compose down

# Run the Redis broker + PostgreSQL backend example
run-redis-postgres-example FRAMEWORK="net11.0":
    dotnet run --project samples/DotCelery.RedisPostgresExample --framework {{FRAMEWORK}}

# Run the Redis broker + Redis backend example
run-redis-example FRAMEWORK="net11.0":
    dotnet run --project samples/DotCelery.RedisPostgresExample --framework {{FRAMEWORK}} -- --result-backend=redis

# Pack NuGet packages
pack:
    dotnet pack -c Release

# Format and build
fb: format build

# Format, build, and test
fbt: format build test

# Run benchmarks
bench FRAMEWORK="net11.0":
    dotnet run --project benchmarks/DotCelery.Benchmarks --framework {{FRAMEWORK}} -c Release

# Run specific benchmark class
bench-filter FILTER FRAMEWORK="net11.0":
    dotnet run --project benchmarks/DotCelery.Benchmarks --framework {{FRAMEWORK}} -c Release -- --filter "*{{FILTER}}*"

# Run benchmarks with memory diagnoser
bench-memory FRAMEWORK="net11.0":
    dotnet run --project benchmarks/DotCelery.Benchmarks --framework {{FRAMEWORK}} -c Release -- --memory

# Run benchmarks with shorter iterations (for quick testing)
bench-quick FRAMEWORK="net11.0":
    dotnet run --project benchmarks/DotCelery.Benchmarks --framework {{FRAMEWORK}} -c Release -- --job short

# Verify dashboard formatting, status labels, HTTP handling, and request races (Node.js 24.3+)
test-dashboard-ui:
    node --test eng/dashboard-ui.test.cjs
