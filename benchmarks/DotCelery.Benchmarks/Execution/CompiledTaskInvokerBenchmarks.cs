using BenchmarkDotNet.Attributes;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Progress;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Registry;

namespace DotCelery.Benchmarks.Execution;

/// <summary>
/// Benchmarks for compiled task invocation.
/// </summary>
[MemoryDiagnoser]
public class CompiledTaskInvokerBenchmarks
{
    private CompiledTaskInvoker _invoker = null!;
    private BenchmarkTaskWithOutput _taskWithOutput = null!;
    private BenchmarkTaskVoid _taskVoid = null!;
    private BenchmarkInput _input = null!;
    private TaskRegistration _registrationWithOutput = null!;
    private TaskRegistration _registrationVoid = null!;
    private ITaskContext _context = null!;

    [GlobalSetup]
    public void Setup()
    {
        _invoker = new CompiledTaskInvoker();

        _taskWithOutput = new BenchmarkTaskWithOutput();
        _taskVoid = new BenchmarkTaskVoid();
        _input = new BenchmarkInput { Value = 42, Message = "benchmark" };

        _registrationWithOutput = new TaskRegistration(
            BenchmarkTaskWithOutput.TaskName,
            typeof(BenchmarkTaskWithOutput),
            typeof(BenchmarkInput),
            typeof(BenchmarkOutput)
        );

        _registrationVoid = new TaskRegistration(
            BenchmarkTaskVoid.TaskName,
            typeof(BenchmarkTaskVoid),
            typeof(BenchmarkInput),
            null
        );

        // Pre-compile both registrations
        _invoker.PreCompile(_registrationWithOutput);
        _invoker.PreCompile(_registrationVoid);

        _context = new MockTaskContext();
    }

    [Benchmark(Baseline = true)]
    public Task<object?> InvokeWithOutput() =>
        _invoker.InvokeAsync(
            _taskWithOutput,
            _input,
            _context,
            _registrationWithOutput,
            CancellationToken.None
        );

    [Benchmark]
    public Task<object?> InvokeVoid() =>
        _invoker.InvokeAsync(
            _taskVoid,
            _input,
            _context,
            _registrationVoid,
            CancellationToken.None
        );

    [Benchmark]
    public async Task InvokeMultiple()
    {
        for (var i = 0; i < 100; i++)
        {
            await _invoker.InvokeAsync(
                _taskWithOutput,
                _input,
                _context,
                _registrationWithOutput,
                CancellationToken.None
            );
        }
    }
}

#region Benchmark Task Types

public sealed class BenchmarkInput
{
    public int Value { get; init; }
    public string Message { get; init; } = string.Empty;
}

public sealed class BenchmarkOutput
{
    public int Result { get; init; }
    public string ProcessedMessage { get; init; } = string.Empty;
}

public sealed class BenchmarkTaskWithOutput : ITask<BenchmarkInput, BenchmarkOutput>
{
    public static string TaskName => "benchmark.with_output";

    public Task<BenchmarkOutput> ExecuteAsync(
        BenchmarkInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        return Task.FromResult(
            new BenchmarkOutput
            {
                Result = input.Value * 2,
                ProcessedMessage = $"Processed: {input.Message}",
            }
        );
    }
}

public sealed class BenchmarkTaskVoid : ITask<BenchmarkInput>
{
    public static string TaskName => "benchmark.void";

    public Task ExecuteAsync(
        BenchmarkInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        // Simulate minimal work
        _ = input.Value * 2;
        return Task.CompletedTask;
    }
}

public sealed class MockTaskContext : ITaskContext
{
    public string TaskId => "benchmark-task-id";
    public string TaskName => "benchmark.task";
    public int RetryCount => 0;
    public int MaxRetries => 3;
    public string Queue => "benchmark-queue";
    public DateTimeOffset SentAt => DateTimeOffset.UtcNow;
    public DateTimeOffset? Eta => null;
    public DateTimeOffset? Expires => null;
    public string? ParentId => null;
    public string? RootId => null;
    public string? CorrelationId => null;
    public string? TenantId => null;
    public string? PartitionKey => null;
    public IReadOnlyDictionary<string, string>? Headers => null;
    public IProgressReporter Progress => new NoOpProgressReporter();

    public void Retry(TimeSpan? countdown = null, Exception? exception = null)
    {
        throw new InvalidOperationException("Retry not supported in benchmarks");
    }

    public Task UpdateStateAsync(TaskState state, object? metadata = null)
    {
        return Task.CompletedTask;
    }

    public T GetRequiredService<T>()
        where T : notnull
    {
        throw new InvalidOperationException("Service resolution not supported in benchmarks");
    }

    private sealed class NoOpProgressReporter : IProgressReporter
    {
        public ValueTask ReportAsync(
            double percentage,
            string? message = null,
            IReadOnlyDictionary<string, object>? data = null,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask ReportAsync(
            ProgressInfo progress,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask ReportItemsAsync(
            long itemsProcessed,
            long totalItems,
            string? message = null,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;

        public ValueTask ReportStepAsync(
            int currentStep,
            int totalSteps,
            string? stepName = null,
            CancellationToken cancellationToken = default
        ) => ValueTask.CompletedTask;
    }
}

#endregion
