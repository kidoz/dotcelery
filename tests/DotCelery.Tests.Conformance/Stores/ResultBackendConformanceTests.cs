using DotCelery.Core.Models;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="ResultBackend"/>.
/// </summary>
public abstract class ResultBackendConformanceTests : StoreConformanceTests
{
    private readonly List<ResultBackend> _backends = [];

    public override async ValueTask DisposeAsync()
    {
        foreach (var backend in _backends)
        {
            await backend.DisposeAsync();
        }

        await base.DisposeAsync();
    }

    [Fact]
    public async Task StoreResultAsync_RoundTripsTheResult()
    {
        var backend = CreateBackend();
        var result = Success("task") with
        {
            Retries = 2,
            Worker = "worker-1",
            Metadata = new Dictionary<string, object> { ["key1"] = "value1", ["key2"] = 42 },
            Exception = new TaskExceptionInfo
            {
                Type = "System.Exception",
                Message = "outer",
                InnerException = new TaskExceptionInfo { Type = "Inner", Message = "inner" },
            },
        };

        await backend.StoreResultAsync(result);
        var stored = await CreateBackend().GetResultAsync("task");

        Assert.NotNull(stored);
        Assert.Equal(result.Result, stored.Result);
        Assert.Equal(result.ContentType, stored.ContentType);
        Assert.Equal(result.CompletedAt, stored.CompletedAt);
        Assert.Equal(result.Duration, stored.Duration);
        Assert.Equal(2, stored.Retries);
        Assert.Equal("worker-1", stored.Worker);
        Assert.Equal("inner", stored.Exception?.InnerException?.Message);
        Assert.Equal("value1", stored.Metadata?["key1"].ToString());
        Assert.Equal("42", stored.Metadata?["key2"].ToString());
        Assert.Equal(TaskState.Success, await backend.GetStateAsync("task"));
    }

    [Fact]
    public async Task GetResultAsync_UnknownTask_ReturnsNull()
    {
        var backend = CreateBackend();

        Assert.Null(await backend.GetResultAsync("unknown"));
        Assert.Null(await backend.GetStateAsync("unknown"));
    }

    [Fact]
    public async Task StoreResultAsync_ReplacesTheResult()
    {
        var backend = CreateBackend();
        await backend.StoreResultAsync(Success("task"));

        await backend.StoreResultAsync(Success("task") with { State = TaskState.Failure });

        Assert.Equal(TaskState.Failure, (await backend.GetResultAsync("task"))?.State);
    }

    [Fact]
    public async Task StoreResultAsync_ExpiresAfterTheExpiry()
    {
        var backend = CreateBackend();
        await backend.StoreResultAsync(Success("short"), TimeSpan.FromMinutes(1));
        await backend.StoreResultAsync(Success("default"));

        Time.Advance(TimeSpan.FromMinutes(1));
        Assert.Null(await backend.GetResultAsync("short"));
        Assert.NotNull(await backend.GetResultAsync("default"));

        Time.Advance(StoreOptions.ResultExpiry);
        Assert.Null(await backend.GetResultAsync("default"));
    }

    [Fact]
    public async Task UpdateStateAsync_RecordsTheStateWithoutAResult()
    {
        var backend = CreateBackend();

        await backend.UpdateStateAsync("task", TaskState.Started);

        Assert.Equal(TaskState.Started, await backend.GetStateAsync("task"));
        Assert.Null(await backend.GetResultAsync("task"));
    }

    [Fact]
    public async Task UpdateStateAsync_Pending_DoesNotMoveATaskBack()
    {
        var backend = CreateBackend();
        await backend.UpdateStateAsync("started", TaskState.Started);
        await backend.StoreResultAsync(Success("done"));

        // The client records a task as pending after publishing it, which can be too late
        await backend.UpdateStateAsync("started", TaskState.Pending);
        await backend.UpdateStateAsync("done", TaskState.Pending);

        Assert.Equal(TaskState.Started, await backend.GetStateAsync("started"));
        Assert.Equal(TaskState.Success, await backend.GetStateAsync("done"));
        Assert.Null(await backend.GetResultAsync("started"));
    }

    [Fact]
    public async Task UpdateStateAsync_DoesNotReplaceAFinalState()
    {
        var backend = CreateBackend();
        await backend.StoreResultAsync(Success("task"));

        await backend.UpdateStateAsync("task", TaskState.Started);
        await backend.UpdateStateAsync("task", TaskState.Revoked);

        Assert.Equal(TaskState.Success, await backend.GetStateAsync("task"));
        Assert.Equal(TaskState.Success, (await backend.GetResultAsync("task"))?.State);
    }

    [Fact]
    public async Task UpdateStateAsync_AfterARetry_KeepsTheRetryResult()
    {
        var backend = CreateBackend();
        await backend.StoreResultAsync(Success("task") with { State = TaskState.Retry });

        await backend.UpdateStateAsync("task", TaskState.Started);

        Assert.Equal(TaskState.Started, await backend.GetStateAsync("task"));
        Assert.Equal(TaskState.Retry, (await backend.GetResultAsync("task"))?.State);
    }

    [Fact]
    public async Task UpdateStateAsync_ConcurrentWithAResult_NeverReplacesTheResult()
    {
        var backend = CreateBackend();

        await Task.WhenAll(
            Task.Run(async () => await backend.StoreResultAsync(Success("task"))),
            Task.Run(async () =>
            {
                for (var i = 0; i < 10; i++)
                {
                    await CreateBackend().UpdateStateAsync("task", TaskState.Progress);
                }
            })
        );

        Assert.Equal(TaskState.Success, await backend.GetStateAsync("task"));
    }

    [Fact]
    public async Task WaitForResultAsync_StoredResult_ReturnsIt()
    {
        var backend = CreateBackend();
        await backend.StoreResultAsync(Success("task"));

        var result = await backend.WaitForResultAsync("task").WaitAsync(Timeout);

        Assert.Equal("task", result.TaskId);
    }

    [Fact]
    public async Task WaitForResultAsync_WaitsForAFinalResult()
    {
        var backend = CreateBackend();
        using var cts = new CancellationTokenSource(Timeout);
        var wait = backend.WaitForResultAsync("task", cancellationToken: cts.Token);

        await backend.StoreResultAsync(Success("task") with { State = TaskState.Retry });
        await Task.Delay(50, cts.Token);
        Assert.False(wait.IsCompleted);

        await backend.StoreResultAsync(Success("task"));
        Assert.Equal(TaskState.Success, (await wait).State);
    }

    [Fact]
    public async Task WaitForResultAsync_ResultStoredByAnotherProcess_IsNotified()
    {
        Assert.SkipWhen(Provider.Notifications is null, "The provider has no notifications");
        using var cts = new CancellationTokenSource(Timeout);
        var wait = CreateBackend().WaitForResultAsync("task", cancellationToken: cts.Token);

        // The clock does not move, so only a notification can wake the waiter. Providers may
        // start listening asynchronously, so keep storing until it is received.
        var worker = CreateBackend();
        while (!wait.IsCompleted)
        {
            await worker.StoreResultAsync(LargeSuccess("task"), cancellationToken: cts.Token);
            await Task.WhenAny(wait, Task.Delay(100, cts.Token));
        }

        Assert.Equal(LargeSuccess("task").Result, (await wait).Result);
    }

    [Fact]
    public async Task WaitForResultAsync_WithoutNotifications_PollsForTheResult()
    {
        var storage = new WithoutNotifications(Provider);
        var wait = CreateBackend(storage).WaitForResultAsync("task", TimeSpan.FromHours(1));

        await CreateBackend(storage).StoreResultAsync(Success("task"));

        var result = await AdvanceUntilAsync(wait, StoreOptions.ResultPollInterval);
        Assert.Equal("task", result.TaskId);
    }

    [Fact]
    public async Task WaitForResultAsync_NoResultInTime_ThrowsTimeoutException()
    {
        var wait = CreateBackend().WaitForResultAsync("task", TimeSpan.FromMinutes(1));

        await Assert.ThrowsAsync<TimeoutException>(() =>
            AdvanceUntilAsync(wait, TimeSpan.FromSeconds(30))
        );
    }

    [Fact]
    public async Task WaitForResultAsync_Cancelled_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        var wait = CreateBackend().WaitForResultAsync("task", cancellationToken: cts.Token);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(Timeout));
    }

    [Fact]
    public async Task DisposeAsync_EndsWaitsAndCanBeRepeated()
    {
        var backend = CreateBackend();
        var wait = backend.WaitForResultAsync("task");

        await backend.DisposeAsync();
        await backend.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait.WaitAsync(Timeout));
    }

    private ResultBackend CreateBackend(IStorageProvider? storage = null)
    {
        var backend = new ResultBackend(storage ?? Provider, CreateOptions(), Time);
        _backends.Add(backend);
        return backend;
    }

    private static TaskResult Success(string taskId) =>
        new()
        {
            TaskId = taskId,
            State = TaskState.Success,
            Result = "{\"value\":42}"u8.ToArray(),
            ContentType = "application/json",
            CompletedAt = Start,
            Duration = TimeSpan.FromMilliseconds(1500),
        };

    // Larger than a PostgreSQL notification payload may be
    private static TaskResult LargeSuccess(string taskId) =>
        Success(taskId) with
        {
            Result = Enumerable.Range(0, 20_000).Select(i => (byte)('a' + (i % 26))).ToArray(),
        };
}
