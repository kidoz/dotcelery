using BenchmarkDotNet.Attributes;
using DotCelery.Backend.InMemory;
using DotCelery.Core.Models;

namespace DotCelery.Benchmarks.Backend;

/// <summary>
/// Benchmarks for in-memory result backend operations.
/// </summary>
[MemoryDiagnoser]
public class InMemoryResultBackendBenchmarks
{
    private InMemoryResultBackend _backend = null!;
    private TaskResult _successResult = null!;
    private TaskResult _failureResult = null!;
    private string _existingTaskId = null!;

    [GlobalSetup]
    public void Setup()
    {
        _backend = new InMemoryResultBackend();

        _successResult = new TaskResult
        {
            TaskId = Guid.NewGuid().ToString("N"),
            State = TaskState.Success,
            Result = new byte[500],
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(150),
            Retries = 0,
            Worker = "worker-1",
        };

        _failureResult = new TaskResult
        {
            TaskId = Guid.NewGuid().ToString("N"),
            State = TaskState.Failure,
            Exception = new TaskExceptionInfo
            {
                Type = "System.InvalidOperationException",
                Message = "Test exception for benchmark",
                StackTrace = new string('x', 1000),
            },
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(50),
            Retries = 2,
            Worker = "worker-1",
        };

        // Pre-store a result for get benchmarks
        _existingTaskId = "existing-task-id";
        _backend
            .StoreResultAsync(
                new TaskResult
                {
                    TaskId = _existingTaskId,
                    State = TaskState.Success,
                    Result = new byte[100],
                    ContentType = "application/json",
                    CompletedAt = DateTimeOffset.UtcNow,
                    Duration = TimeSpan.FromMilliseconds(100),
                }
            )
            .AsTask()
            .Wait();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _backend.DisposeAsync();
    }

    [Benchmark(Baseline = true)]
    public ValueTask StoreSuccessResult() => _backend.StoreResultAsync(_successResult);

    [Benchmark]
    public ValueTask StoreFailureResult() => _backend.StoreResultAsync(_failureResult);

    [Benchmark]
    public ValueTask<TaskResult?> GetExistingResult() => _backend.GetResultAsync(_existingTaskId);

    [Benchmark]
    public ValueTask<TaskResult?> GetNonExistentResult() =>
        _backend.GetResultAsync("non-existent-task-id");

    [Benchmark]
    public ValueTask UpdateState() => _backend.UpdateStateAsync(_existingTaskId, TaskState.Started);

    [Benchmark]
    public ValueTask<TaskState?> GetState() => _backend.GetStateAsync(_existingTaskId);

    [Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    [Arguments(1000)]
    public async Task StoreBatch(int count)
    {
        for (var i = 0; i < count; i++)
        {
            var result = _successResult with { TaskId = $"batch-task-{i}" };
            await _backend.StoreResultAsync(result);
        }
    }
}

/// <summary>
/// Benchmarks for result waiting patterns.
/// </summary>
[MemoryDiagnoser]
public class ResultWaitingBenchmarks
{
    private InMemoryResultBackend _backend = null!;

    [GlobalSetup]
    public void Setup()
    {
        _backend = new InMemoryResultBackend();
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _backend.DisposeAsync();
    }

    [Benchmark]
    public async Task StoreAndWait()
    {
        var taskId = Guid.NewGuid().ToString("N");
        var result = new TaskResult
        {
            TaskId = taskId,
            State = TaskState.Success,
            Result = new byte[100],
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(50),
        };

        // Store result first, then wait should return immediately
        await _backend.StoreResultAsync(result);
        await _backend.WaitForResultAsync(taskId, TimeSpan.FromSeconds(1));
    }

    [Benchmark]
    public async Task ConcurrentStoreAndWait()
    {
        var taskId = Guid.NewGuid().ToString("N");
        var result = new TaskResult
        {
            TaskId = taskId,
            State = TaskState.Success,
            Result = new byte[100],
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(50),
        };

        // Start waiting before storing (simulates concurrent producer/consumer)
        var waitTask = _backend.WaitForResultAsync(taskId, TimeSpan.FromSeconds(1));

        // Small delay then store
        await Task.Delay(1);
        await _backend.StoreResultAsync(result);

        await waitTask;
    }
}
