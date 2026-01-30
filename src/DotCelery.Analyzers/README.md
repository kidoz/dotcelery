# DotCelery.Analyzers

Roslyn analyzers to prevent common usage errors in DotCelery at compile time.

## Installation

```bash
dotnet add package DotCelery.Analyzers
```

The analyzers will automatically be included in your project and run during compilation.

## Diagnostic Rules

### Task Definition Issues (DCEL001-099)

#### DCEL001: Task name cannot be empty
**Severity**: Error

TaskName must be a non-empty string for proper routing.

```csharp
// ❌ Bad
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => ""; // DCEL001
}

// ✅ Good
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}
```

#### DCEL002: Duplicate task name detected
**Severity**: Error

Each task must have a unique name across the application.

```csharp
// ❌ Bad
public sealed class Task1 : ITask<Input, Output>
{
    public static string TaskName => "duplicate"; // DCEL002
}

public sealed class Task2 : ITask<Input, Output>
{
    public static string TaskName => "duplicate"; // DCEL002
}

// ✅ Good
public sealed class Task1 : ITask<Input, Output>
{
    public static string TaskName => "task.one";
}

public sealed class Task2 : ITask<Input, Output>
{
    public static string TaskName => "task.two";
}
```

#### DCEL003: Task classes should be sealed
**Severity**: Warning
**Code Fix**: Available

Tasks should be sealed to prevent inheritance issues.

```csharp
// ❌ Bad
public class MyTask : ITask<Input, Output> // DCEL003
{
    public static string TaskName => "my.task";
}

// ✅ Good
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}
```

### Async/Await Issues (DCEL100-199)

#### DCEL100: Avoid blocking calls in async task execution
**Severity**: Warning

Blocking calls like `Task.Wait()`, `Task.Result`, or `.GetAwaiter().GetResult()` can cause deadlocks. Use `await` instead.

```csharp
// ❌ Bad
public sealed class MyTask : ITask<Input, Output>
{
    public async Task<Output> ExecuteAsync(Input input, ITaskContext context, CancellationToken ct)
    {
        var task = GetDataAsync();
        task.Wait(); // DCEL100
        var result = task.Result; // DCEL100
        var data = task.GetAwaiter().GetResult(); // DCEL100
        return new Output();
    }
}

// ✅ Good
public sealed class MyTask : ITask<Input, Output>
{
    public async Task<Output> ExecuteAsync(Input input, ITaskContext context, CancellationToken ct)
    {
        var data = await GetDataAsync();
        return new Output();
    }
}
```

### Attribute Validation (DCEL200-299)

#### DCEL200: Invalid time limit configuration
**Severity**: Error

Time limits must be positive and SoftLimit must be less than HardLimit.

```csharp
// ❌ Bad
[TimeLimit(SoftLimitSeconds = -1, HardLimitSeconds = 60)] // DCEL200
[TimeLimit(SoftLimitSeconds = 100, HardLimitSeconds = 50)] // DCEL200
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}

// ✅ Good
[TimeLimit(SoftLimitSeconds = 30, HardLimitSeconds = 60)]
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}
```

#### DCEL201: Invalid route queue name
**Severity**: Error

Route attribute queue name cannot be null, empty, or whitespace.

```csharp
// ❌ Bad
[Route("")] // DCEL201
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}

// ✅ Good
[Route("high-priority")]
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}
```

#### DCEL202: Invalid PreventOverlapping configuration
**Severity**: Error

PreventOverlapping must be configured correctly (TimeoutSeconds > 0, KeyProperty only with KeyByInput).

```csharp
// ❌ Bad
[PreventOverlapping(TimeoutSeconds = -1)] // DCEL202
[PreventOverlapping(KeyProperty = "Id")] // DCEL202 - KeyByInput not set
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}

// ✅ Good
[PreventOverlapping(TimeoutSeconds = 3600)]
[PreventOverlapping(KeyByInput = true, KeyProperty = "Id")]
public sealed class MyTask : ITask<Input, Output>
{
    public static string TaskName => "my.task";
}
```

### Serialization Issues (DCEL300-399)

#### DCEL300: Type must be serializable
**Severity**: Warning

Task input and output types must be serializable. Classes should have a parameterless constructor or be records.

```csharp
// ❌ Bad
public class Input // DCEL300 - no parameterless constructor
{
    public Input(string value) { Value = value; }
    public string Value { get; }
}

// ✅ Good
public class Input
{
    public string Value { get; set; }
}

// ✅ Also Good - using records
public record Input(string Value);
```

#### DCEL301: Avoid mutable collections in task input/output
**Severity**: Info

Mutable collections can lead to race conditions in distributed scenarios. Consider using immutable collections.

```csharp
// ⚠️ Warning
public class Input
{
    public List<string> Items { get; set; } // DCEL301
    public Dictionary<string, int> Counts { get; set; } // DCEL301
}

// ✅ Better
public class Input
{
    public IReadOnlyList<string> Items { get; init; }
    public IReadOnlyDictionary<string, int> Counts { get; init; }
}
```

### Usage Issues (DCEL400-499)

#### DCEL400: SendAsync type parameters do not match task definition
**Severity**: Error

The input and output types specified in SendAsync must match the task's generic parameters.

```csharp
public sealed class MyTask : ITask<RealInput, RealOutput>
{
    public static string TaskName => "my.task";
    // ...
}

// ❌ Bad
await celery.SendAsync<MyTask, WrongInput, WrongOutput>(new WrongInput()); // DCEL400

// ✅ Good
await celery.SendAsync<MyTask, RealInput, RealOutput>(new RealInput());
```

#### DCEL401: Task not registered in DI container
**Severity**: Info

Tasks must be registered in the DI container to be executed by workers.

```csharp
// ⚠️ Info
await celery.SendAsync<MyTask, Input, Output>(new Input()); // DCEL401

// ✅ Good - ensure task is registered
services.AddDotCelery(options => { })
    .AddTask<MyTask>();
```

## Suppressing Diagnostics

To suppress a specific diagnostic, use the `#pragma` directive or a suppression attribute:

```csharp
#pragma warning disable DCEL100
task.Wait(); // I know what I'm doing
#pragma warning restore DCEL100
```

Or in `.editorconfig`:

```ini
[*.cs]
dotnet_diagnostic.DCEL100.severity = none
```

## Contributing

To add new analyzers:

1. Add diagnostic descriptor to `DiagnosticDescriptors.cs`
2. Create analyzer class implementing `DiagnosticAnalyzer`
3. Create code fix provider if applicable (in `CodeFixes/` directory)
4. Add tests in `DotCelery.Analyzers.Tests`

See existing analyzers for examples.
