# Contributing to DotCelery

Thank you for your interest in contributing to DotCelery! This document provides guidelines and information for contributors.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Coding Standards](#coding-standards)
- [C# 14 Features](#c-14-features)
- [Testing Guidelines](#testing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Commit Message Format](#commit-message-format)

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/dotcelery.git`
3. Create a feature branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Run tests: `dotnet test tests/DotCelery.Tests.Unit`
6. Push and create a Pull Request

## Development Setup

### Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)
- [Docker](https://www.docker.com/) (for integration tests)
- IDE: Visual Studio 2026, JetBrains Rider, or VS Code with C# Dev Kit

### Build

```bash
# Restore and build
dotnet build

# Run unit tests
dotnet test tests/DotCelery.Tests.Unit

# Run integration tests (requires Docker)
dotnet test tests/DotCelery.Tests.Integration
```

### Project Structure

```
dotcelery/
├── src/
│   ├── DotCelery.Core/          # Core abstractions and models
│   ├── DotCelery.Client/        # Task sending client
│   ├── DotCelery.Worker/        # Worker implementation
│   ├── DotCelery.Beat/          # Periodic task scheduler
│   ├── DotCelery.Cron/          # Cron expression parser
│   ├── DotCelery.Dashboard/     # Web monitoring UI
│   ├── DotCelery.Broker.*/      # Message broker implementations
│   ├── DotCelery.Backend.*/     # Result backend implementations
│   └── DotCelery.Telemetry/     # OpenTelemetry instrumentation
└── tests/
    ├── DotCelery.Tests.Unit/        # Unit tests (xUnit v3)
    └── DotCelery.Tests.Integration/ # Integration tests (Testcontainers)
```

## Coding Standards

### Required Patterns

| Pattern | Description |
|---------|-------------|
| File-scoped namespaces | All files must use file-scoped namespaces |
| Primary constructors | Use for DI injection where appropriate |
| Nullable reference types | Always enabled (`<Nullable>enable</Nullable>`) |
| XML documentation | Required for all public APIs |

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

## Testing Guidelines

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

### Integration Tests

- Location: `tests/DotCelery.Tests.Integration/`
- Requires: Docker (Testcontainers)
- Tests real broker/backend interactions

### Running Tests

```bash
# Unit tests only (fast, no Docker)
dotnet test tests/DotCelery.Tests.Unit

# Integration tests (requires Docker)
dotnet test tests/DotCelery.Tests.Integration

# All tests
dotnet test

# Specific test class
dotnet test --filter "FullyQualifiedName~ChainTests"
```

## Pull Request Process

### Before Submitting

1. **Run all unit tests**: `dotnet test tests/DotCelery.Tests.Unit`
2. **Build without warnings**: `dotnet build --warnaserror`
3. **Add tests** for new functionality
4. **Update documentation** if adding public APIs

### PR Requirements

- [ ] All unit tests pass
- [ ] No new warnings
- [ ] XML documentation for public APIs
- [ ] Tests cover new functionality
- [ ] Follows coding standards

### Review Process

1. Create PR against `main` branch
2. Fill out the PR template
3. Address reviewer feedback
4. Squash commits if requested
5. Maintainer will merge when approved

## Commit Message Format

Use conventional commits:

```
<type>(<scope>): <subject>

[optional body]

[optional footer]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Code style (formatting, no logic change) |
| `refactor` | Code refactoring |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `chore` | Build process, dependencies |

### Examples

```
feat(canvas): add + operator for Chain concatenation

fix(cron): handle DST gaps in GetNextOccurrence

docs(readme): update installation instructions

test(chain): add tests for params Span constructor
```

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions
- Check existing issues before creating new ones

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
