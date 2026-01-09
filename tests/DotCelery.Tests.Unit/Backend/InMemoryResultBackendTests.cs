using DotCelery.Backend.InMemory;
using DotCelery.Core.Models;

namespace DotCelery.Tests.Unit.Backend;

public class InMemoryResultBackendTests : IAsyncDisposable
{
    private readonly InMemoryResultBackend _backend = new();

    [Fact]
    public async Task StoreResultAsync_ValidResult_CanBeRetrieved()
    {
        var result = CreateSuccessResult();

        await _backend.StoreResultAsync(result);
        var retrieved = await _backend.GetResultAsync(result.TaskId);

        Assert.NotNull(retrieved);
        Assert.Equal(result.TaskId, retrieved.TaskId);
        Assert.Equal(result.State, retrieved.State);
    }

    [Fact]
    public async Task GetResultAsync_NonExistentTaskId_ReturnsNull()
    {
        var result = await _backend.GetResultAsync("non-existent-id");

        Assert.Null(result);
    }

    [Fact]
    public async Task WaitForResultAsync_ExistingResult_ReturnsImmediately()
    {
        var result = CreateSuccessResult();
        await _backend.StoreResultAsync(result);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var retrieved = await _backend.WaitForResultAsync(
            result.TaskId,
            cancellationToken: cts.Token
        );

        Assert.NotNull(retrieved);
        Assert.Equal(result.TaskId, retrieved.TaskId);
    }

    [Fact]
    public async Task WaitForResultAsync_ResultStoredLater_WaitsAndReturns()
    {
        var result = CreateSuccessResult();

        // Store result after a delay
        _ = Task.Run(async () =>
        {
            await Task.Delay(100);
            await _backend.StoreResultAsync(result);
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var retrieved = await _backend.WaitForResultAsync(
            result.TaskId,
            cancellationToken: cts.Token
        );

        Assert.NotNull(retrieved);
        Assert.Equal(result.TaskId, retrieved.TaskId);
    }

    [Fact]
    public async Task WaitForResultAsync_Timeout_ThrowsTimeoutException()
    {
        await Assert.ThrowsAsync<TimeoutException>(() =>
            _backend.WaitForResultAsync("non-existent", timeout: TimeSpan.FromMilliseconds(50))
        );
    }

    [Fact]
    public async Task WaitForResultAsync_Cancelled_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // TaskCanceledException inherits from OperationCanceledException
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            _backend.WaitForResultAsync("non-existent", cancellationToken: cts.Token)
        );
    }

    [Fact]
    public async Task UpdateStateAsync_ValidState_UpdatesCorrectly()
    {
        var taskId = Guid.NewGuid().ToString();

        await _backend.UpdateStateAsync(taskId, TaskState.Started);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Started, state);
    }

    [Fact]
    public async Task GetStateAsync_NonExistentTaskId_ReturnsNull()
    {
        var state = await _backend.GetStateAsync("non-existent");

        Assert.Null(state);
    }

    [Fact]
    public async Task StoreResultAsync_OverwritesExisting()
    {
        var taskId = Guid.NewGuid().ToString();
        var result1 = CreateSuccessResult() with { TaskId = taskId };
        var result2 = CreateFailureResult() with { TaskId = taskId };

        await _backend.StoreResultAsync(result1);
        await _backend.StoreResultAsync(result2);

        var retrieved = await _backend.GetResultAsync(taskId);
        Assert.NotNull(retrieved);
        Assert.Equal(TaskState.Failure, retrieved.State);
    }

    [Fact]
    public async Task StoreResultAsync_NotifiesWaiters()
    {
        var result = CreateSuccessResult();

        // Start waiting before storing
        var waitTask = _backend.WaitForResultAsync(result.TaskId, timeout: TimeSpan.FromSeconds(5));

        // Store after a small delay
        await Task.Delay(50);
        await _backend.StoreResultAsync(result);

        var retrieved = await waitTask;
        Assert.Equal(result.TaskId, retrieved.TaskId);
    }

    [Fact]
    public void Clear_RemovesAllData()
    {
        // This tests the Clear method specific to InMemoryResultBackend
        _backend.Clear();

        Assert.Equal(0, _backend.Count);
    }

    [Fact]
    public async Task Count_ReturnsCorrectCount()
    {
        await _backend.StoreResultAsync(CreateSuccessResult());
        await _backend.StoreResultAsync(CreateSuccessResult());

        Assert.Equal(2, _backend.Count);
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        await _backend.DisposeAsync();
        await _backend.DisposeAsync(); // Should not throw
    }

    public async ValueTask DisposeAsync()
    {
        await _backend.DisposeAsync();
    }

    private static TaskResult CreateSuccessResult() =>
        new()
        {
            TaskId = Guid.NewGuid().ToString(),
            State = TaskState.Success,
            Result = "{\"value\": 42}"u8.ToArray(),
            ContentType = "application/json",
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
        };

    private static TaskResult CreateFailureResult() =>
        new()
        {
            TaskId = Guid.NewGuid().ToString(),
            State = TaskState.Failure,
            Exception = new TaskExceptionInfo
            {
                Type = "System.Exception",
                Message = "Test exception",
            },
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromMilliseconds(100),
        };
}
