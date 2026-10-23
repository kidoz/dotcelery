# Contributing to DotCelery

Thank you for your interest in contributing to DotCelery! This document provides guidelines and information for contributors.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Documentation](#documentation)
- [Coding Standards](#coding-standards)
- [C# 14 Features](#c-14-features)
- [Native AOT and Trimming](#native-aot-and-trimming)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Commit Message Format](#commit-message-format)

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/dotcelery.git`
3. Create a feature branch: `git checkout -b feature/your-feature-name`
4. Restore the local tools: `dotnet tool restore`
5. Make your changes
6. Format the code: `dotnet csharpier format .`
7. Run the container-free tests: `dotnet test tests/DotCelery.Tests.Unit`
8. Push and create a Pull Request

## Development Setup

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Node.js 24.3 or later](https://nodejs.org/) (builds the embedded dashboard)
- [Docker](https://www.docker.com/) (for integration tests)
- IDE: Visual Studio 2026, JetBrains Rider, or VS Code with C# Dev Kit
- Optional: [just](https://github.com/casey/just) for the shortcuts in the `justfile`

### Build

```bash
# Restore the local tools (CSharpier) and dependencies
dotnet tool restore
dotnet restore

# Build
dotnet build

# Format the code
dotnet csharpier format .

# Check formatting without changing files
dotnet csharpier check .

# Run unit tests
dotnet test tests/DotCelery.Tests.Unit
```

The `justfile` wraps the common commands:

| Task | Direct command | `just` recipe |
|------|----------------|---------------|
| Format | `dotnet csharpier format .` | `just format` |
| Check formatting | `dotnet csharpier check .` | `just format-check` |
| Build | `dotnet build` | `just build` |
| Unit tests | `dotnet test tests/DotCelery.Tests.Unit` | `just test` |
| All tests | `dotnet test` | `just test-all` |
| Native AOT smoke test | see [Native AOT and Trimming](#native-aot-and-trimming) | `just aot-smoke` |
| Benchmarks | `dotnet run --project benchmarks/DotCelery.Benchmarks -c Release` | `just bench` |

### Dashboard frontend checks

Follow [Develop the dashboard](docs/how-to/develop-dashboard.md) for hot reload,
formatting, linting, and browser checks. The [architecture explanation](docs/explanation/dashboard-architecture.md)
describes module boundaries, polling, and embedded assets.

### Dashboard REST API versions

The API requires an explicit `/api/v1/` URL. Use the
[REST API reference](docs/reference/dashboard-api.md) for versioning integration,
endpoint contracts, and the compatibility policy, and
[Host the dashboard](docs/how-to/host-dashboard.md) for application setup.

### Project Structure

```
dotcelery/
├── src/
│   ├── DotCelery.Core/              # Core abstractions, models, canvas and storage primitives
│   ├── DotCelery.Client/            # Task sending client
│   ├── DotCelery.Worker/            # Worker, task execution, sagas, resilience, progress
│   ├── DotCelery.Beat/              # Periodic task scheduler
│   ├── DotCelery.Cron/              # Cron expression parser
│   ├── DotCelery.Dashboard/         # Web monitoring UI
│   ├── DotCelery.Analyzers/         # Roslyn analyzers and the task registration source generator
│   ├── DotCelery.Broker.InMemory/   # In-memory broker (testing)
│   ├── DotCelery.Broker.RabbitMQ/   # RabbitMQ broker
│   ├── DotCelery.Broker.Redis/      # Redis Streams broker
│   ├── DotCelery.Broker.Sqs/        # Amazon SQS broker
│   ├── DotCelery.Backend.InMemory/  # In-memory result backend and storage (testing)
│   ├── DotCelery.Backend.Redis/     # Redis result backend and stores
│   ├── DotCelery.Backend.Postgres/  # PostgreSQL result backend and stores
│   ├── DotCelery.Backend.SqlServer/ # SQL Server result backend and stores
│   ├── DotCelery.Backend.Mongo/     # MongoDB result backend and stores
│   ├── DotCelery.Storage.Sql/       # SQL schema model, migrations, and SQL storage primitives
│   └── DotCelery.Telemetry/         # OpenTelemetry instrumentation
├── tests/
│   ├── DotCelery.Tests.Unit/        # Unit tests (xUnit v3)
│   ├── DotCelery.Tests.Integration/ # Integration tests (Testcontainers)
│   ├── DotCelery.Tests.Generated/   # Compiles the source generator against real tasks
│   ├── DotCelery.Analyzers.Tests/   # Analyzer and generator tests
│   └── DotCelery.Tests.Conformance/ # Shared conformance suites, referenced by the test projects
├── samples/
│   ├── DotCelery.Demo/              # Minimal worker and client example
│   ├── DotCelery.CanvasExample/     # Chain, group, and chord workflows
│   ├── DotCelery.Dashboard.Demo/    # Dashboard example
│   ├── DotCelery.RedisPostgresExample/ # Redis broker with a PostgreSQL backend
│   └── DotCelery.AotSmoke/          # Native AOT smoke test
└── benchmarks/
    └── DotCelery.Benchmarks/        # BenchmarkDotNet benchmarks
```

## Documentation

Product documentation lives under [docs/](docs/README.md), organized into
Diátaxis tutorials, how-to guides, reference, and explanation. Follow
[Add or update documentation](docs/how-to/write-documentation.md) to choose a form,
connect navigation, and verify examples. Keep this file as the entry point for
repository development conventions.

## Coding Standards

### Required Patterns

| Pattern | Description |
|---------|-------------|
| File-scoped namespaces | All files must use file-scoped namespaces |
| Primary constructors | Use for DI injection where appropriate |
| Nullable reference types | Always enabled (`<Nullable>enable</Nullable>`) |
| XML documentation | Required for all public APIs |
| Source generation over reflection | Prefer generated code for registration, invocation, and serialization |
| AOT annotations | Annotate reflection with `[RequiresUnreferencedCode]` and, when code is compiled at run time, `[RequiresDynamicCode]` |

### Formatting

Formatting is enforced by [CSharpier](https://csharpier.com/) through the local tool defined in
`.config/dotnet-tools.json` and configured in `.csharpierrc.yaml` (print width 100, four-space
indentation, LF line endings). CI fails when `dotnet csharpier check .` reports a difference, so
run `dotnet csharpier format .` before committing.

The build also enforces code quality:

- `TreatWarningsAsErrors` is enabled, so a warning fails the build.
- `AnalysisMode` is `Recommended` and `EnforceCodeStyleInBuild` is enabled.
- Suppressions belong in `Directory.Build.props` or `src/Directory.Build.targets` only when they
  apply project-wide and have a stated reason; do not suppress a diagnostic just to make a build
  pass.

### Public API

Every shipped library tracks its public surface in `PublicAPI.Shipped.txt` and
`PublicAPI.Unshipped.txt` (`Microsoft.CodeAnalysis.PublicApiAnalyzers`, enabled for everything
under `src/`). Adding, removing, or changing a public member fails the build with `RS0016`,
`RS0017`, or `RS0018` until the change is declared, so an accidental API change cannot ship.

After an intentional API change, declare it with:

```bash
just api-format
```

This adds new members to `PublicAPI.Unshipped.txt`. Move entries from `Unshipped` to `Shipped`
only when the change is part of a release; both files are enforced.

### Naming Conventions

```csharp
// Interfaces: prefix with I
public interface IMessageBroker { }

// Async methods: suffix with Async
public Task<T> GetResultAsync(string taskId, CancellationToken ct);

// Private fields: prefix with underscore
private readonly IMessageBroker _broker;

// Constants: PascalCase
public const string DefaultQueue = "celery";
```

### Prohibited Patterns

- Reflection in hot paths (use source generators)
- Runtime reflection in AOT-compatible code without a `[RequiresUnreferencedCode]` annotation
- `Expression.Compile` on paths that must work under Native AOT (the source generator emits a typed invoker instead)
- `async void` methods (except event handlers)
- Blocking calls in async contexts (`Task.Result`, `Task.Wait()`)
- Public mutable state without synchronization

## C# 14 Features

DotCelery leverages modern C# 14 features. Use these patterns:

### Properties with `field` Keyword

```csharp
// Use field keyword for validation
public int Threshold
{
    get;
    set => field = value > 0
        ? value
        : throw new ArgumentOutOfRangeException(nameof(value));
} = 10;
```

### `params ReadOnlySpan<T>` for Zero-Allocation

```csharp
// Prefer params Span for variadic methods
public Chain(params ReadOnlySpan<Signature> tasks) { }

// Usage: new Chain(sig1, sig2, sig3) - no array allocation
```

### Operators in Classes

```csharp
// Add operators for fluent APIs
public static Chain operator +(Chain chain, Signature sig)
{
    chain.Then(sig);
    return chain;
}
```

### Other Modern Features

- `System.Threading.Lock` for synchronization (not `object`)
- `FrozenDictionary<K,V>` for read-heavy registries
- Pattern matching for null checks
- Collection expressions `[item1, item2]`

## Native AOT and Trimming

`DotCelery.Core`, `DotCelery.Worker`, `DotCelery.Client`, `DotCelery.Broker.InMemory`, and
`DotCelery.Backend.InMemory` are marked `IsAotCompatible` and must compile without AOT or trim
warnings. The dashboard and the driver-based providers (RabbitMQ, Redis, SQS, PostgreSQL, SQL
Server, MongoDB) are not covered by this verification.

When changing an AOT-compatible project:

- Avoid reflection. Use the source generator for task registration, typed invocation, and JSON
  serialization.
- Annotate a public API that unavoidably needs reflection with `[RequiresUnreferencedCode]` and,
  when it compiles code at run time, `[RequiresDynamicCode]`, so an AOT or trimmed consumer gets a
  warning instead of a runtime failure.
- Register task payload types in a source-generated `JsonSerializerContext`. DotCelery's own model
  types come from its built-in context; an unregistered payload type is an error, not a reflection
  fallback.
- Do not add `IsAotCompatible` to a project until it compiles cleanly with the AOT and trim
  analyzers.
- Keep `AddTasksFromAssembly`, `UsePreventOverlapping`, and `UseQueuedSignalDispatch`
  annotated; they require reflection or dynamic code.

Verify an AOT change by publishing and running the smoke sample:

```bash
# Uses the host RID, publishes with PublishAot, and runs the result
just aot-smoke
```

CI does the same for `linux-x64` after installing `clang` and `zlib1g-dev`. The sample registers
its task through the generated `RegisterTasks()`, serializes through a generated JSON context, and
runs a task end to end.

## Testing Guidelines

### Test Suites

| Suite | Project | Requirements |
|-------|---------|--------------|
| Unit | `tests/DotCelery.Tests.Unit/` | None; uses the in-memory broker and backend |
| Generated | `tests/DotCelery.Tests.Generated/` | None; runs the source generator over real task types |
| Analyzers | `tests/DotCelery.Analyzers.Tests/` | None; runs analyzers and the generator against test compilations |
| Integration | `tests/DotCelery.Tests.Integration/` | Docker (Testcontainers) |
| Conformance | `tests/DotCelery.Tests.Conformance/` | Not run directly; shared suites referenced by the Unit and Integration projects |

### Unit Tests

- Location: `tests/DotCelery.Tests.Unit/`
- Framework: xUnit v3
- Naming: `ClassName_MethodName_ExpectedBehavior`

```csharp
[Fact]
public void Chain_PlusOperator_AppendsSignature()
{
    // Arrange
    var chain = new Chain([new Signature { TaskName = "task1" }]);
    var sig = new Signature { TaskName = "task2" };

    // Act
    var result = chain + sig;

    // Assert
    Assert.Equal(2, result.Tasks.Count);
}
```

Prefer the in-memory broker and backend for tests that do not need a real external service. When a
store or broker gains behavior, add it to the shared conformance suite rather than only to one
provider's tests.

### Analyzer and Generator Tests

Changes to `src/DotCelery.Analyzers/` need tests in two places:

- `tests/DotCelery.Analyzers.Tests/` for analyzer diagnostics and generator output. Add both a
  positive case (the diagnostic is reported) and a negative case (valid code produces nothing).
- `tests/DotCelery.Tests.Generated/` when the generator's emitted registration or invocation
  changes. This project compiles the generator over real public, internal, and nested tasks, so it
  proves the emitted code compiles and registers the tasks.

### Integration Tests

- Location: `tests/DotCelery.Tests.Integration/`
- Requires: Docker (Testcontainers)
- Covers Redis, PostgreSQL, SQL Server, RabbitMQ, MongoDB, and SQS (through LocalStack)
- Tests real broker/backend interactions

SQL Server images are x86-64 only, so on ARM machines Docker must emulate them (Docker Desktop:
"Use Rosetta for x86_64/amd64 emulation").

Run the container-free suites and the integration suite one at a time. Starting several
`dotnet test` processes at once can exhaust the Docker daemon and cause container startup
timeouts.

### Running Tests

```bash
# Container-free suites
dotnet test tests/DotCelery.Tests.Unit
dotnet test tests/DotCelery.Tests.Generated
dotnet test tests/DotCelery.Analyzers.Tests

# Integration tests (requires Docker)
dotnet test tests/DotCelery.Tests.Integration

# All tests (requires Docker)
dotnet test

# Specific test class
dotnet test --filter "FullyQualifiedName~ChainTests"
```

## Pull Request Process

### Before Submitting

1. **Format the code**: `dotnet csharpier format .`
2. **Build in Release**: `dotnet build -c Release` (warnings are errors)
3. **Run the container-free suites**: Unit, Generated, and Analyzers
4. **Run the integration suite** if you changed a broker, backend, storage provider, or worker path
   that talks to one: `dotnet test tests/DotCelery.Tests.Integration`
5. **Run the AOT smoke test** if you changed an AOT-compatible project: `just aot-smoke`
6. **Add tests** for new functionality
7. **Update documentation** if adding public APIs, and add a `CHANGELOG.md` entry under
   `[Unreleased]` for user-visible changes

### PR Requirements

- [ ] Formatting passes (`dotnet csharpier check .`)
- [ ] Build succeeds in Release with no warnings
- [ ] Unit, Generated, and Analyzer tests pass
- [ ] Integration tests pass when broker, backend, or storage behavior changes
- [ ] Native AOT smoke test passes when an AOT-compatible project changes
- [ ] XML documentation for public APIs
- [ ] Tests cover new functionality
- [ ] Follows coding standards

### Continuous Integration

| Workflow | What it runs |
|----------|--------------|
| `build.yml` | Tool restore, formatting check, vulnerability audit, Release build, Unit/Generated/Analyzer tests, and a `linux-x64` Native AOT publish and run |
| `integration.yml` | The Testcontainers integration suite |
| `packaging.yml` | Release build and `dotnet pack` of the solution |
| `release.yml` | Strict semver tag and `CHANGELOG.md` validation, the integration suite, tests, packing, provenance attestation, and NuGet publishing |

### Review Process

1. Create PR against `main` branch
2. Describe the change and how you verified it
3. Address reviewer feedback
4. Squash commits if requested
5. Maintainer will merge when approved

## Commit Message Format

Commits are a single line in the imperative mood, in sentence case, without a trailing period:

```text
Add Redis broker reconnect tests
Fix cron DST gap handling
Document generated task types and signature helpers
```

- No conventional-commit prefix or scope (`feat:`, `fix(cron):`), ticket tag, or emoji.
- No body and no trailers (`Co-Authored-By:`, `Signed-off-by:`, or generator footers).
- Keep each commit focused; separate refactoring from behavior changes.

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions
- Check existing issues before creating new ones

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
