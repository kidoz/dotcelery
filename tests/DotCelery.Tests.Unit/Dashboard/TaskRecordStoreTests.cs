using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Services;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Exercises the task record store and the summary provider the dashboard reads from: records
/// are written, updated as a task moves through its states, listed by state and name, and
/// counted.
/// </summary>
public sealed class TaskRecordStoreTests
{
    private readonly InMemoryStorageProvider _storage = new();
    private readonly TaskRecordStore _store;

    public TaskRecordStoreTests()
    {
        _store = new TaskRecordStore(Options.Create(new DashboardOptions()), _storage);
    }

    [Fact]
    public async Task ARunningTask_IsRecordedAndCanBeReadBack()
    {
        var summary = CreateSummary("t1", "tests.alpha", TaskState.Started);

        await _store.RecordAsync(summary);

        var stored = await _store.GetAsync("t1");
        Assert.NotNull(stored);
        Assert.Equal("tests.alpha", stored.TaskName);
        Assert.Equal(TaskState.Started.ToString(), stored.State);
    }

    [Fact]
    public async Task ATaskThatChangesState_IsListedUnderItsLatestState()
    {
        await _store.RecordAsync(CreateSummary("t1", "tests.alpha", TaskState.Started));
        await _store.RecordAsync(CreateSummary("t2", "tests.beta", TaskState.Started));

        // t1 finishes; t2 is still running
        await _store.RecordAsync(
            CreateSummary("t1", "tests.alpha", TaskState.Success) with
            {
                CompletedAt = DateTimeOffset.UtcNow,
            }
        );

        var running = await CollectAsync(_store.QueryByStateAsync(TaskState.Started, 50, 0));
        Assert.Equal("t2", Assert.Single(running).TaskId);

        var succeeded = await CollectAsync(_store.QueryByStateAsync(TaskState.Success, 50, 0));
        Assert.Equal("t1", Assert.Single(succeeded).TaskId);
    }

    [Fact]
    public async Task Records_ArePagedNewestFirst()
    {
        var now = DateTimeOffset.UtcNow;
        await _store.RecordAsync(
            CreateSummary("old", "tests.alpha", TaskState.Success) with
            {
                CompletedAt = now.AddMinutes(-5),
            }
        );
        await _store.RecordAsync(
            CreateSummary("new", "tests.alpha", TaskState.Success) with
            {
                CompletedAt = now,
            }
        );

        var page = await CollectAsync(_store.QueryByStateAsync(TaskState.Success, 1, 0));
        Assert.Equal("new", Assert.Single(page).TaskId);

        var second = await CollectAsync(_store.QueryByStateAsync(TaskState.Success, 1, 1));
        Assert.Equal("old", Assert.Single(second).TaskId);
    }

    [Fact]
    public async Task Tasks_AreListedByName()
    {
        await _store.RecordAsync(CreateSummary("t1", "tests.alpha", TaskState.Success));
        await _store.RecordAsync(CreateSummary("t2", "tests.beta", TaskState.Success));
        await _store.RecordAsync(CreateSummary("t3", "tests.alpha", TaskState.Failure));

        var alpha = await CollectAsync(_store.QueryByNameAsync("tests.alpha", 50, 0));
        Assert.Equal(2, alpha.Count);
        Assert.All(alpha, summary => Assert.Equal("tests.alpha", summary.TaskName));
    }

    [Fact]
    public async Task StateCounts_AndTaskNames_CoverTheRecords()
    {
        await _store.RecordAsync(CreateSummary("t1", "tests.alpha", TaskState.Success));
        await _store.RecordAsync(CreateSummary("t2", "tests.alpha", TaskState.Success));
        await _store.RecordAsync(CreateSummary("t3", "tests.beta", TaskState.Failure));

        var counts = await _store.GetStateCountsAsync();
        Assert.Equal(2, counts[TaskState.Success]);
        Assert.Equal(1, counts[TaskState.Failure]);
        Assert.Equal(0, counts[TaskState.Started]);

        var names = await _store.GetTaskNamesAsync();
        Assert.Equal(
            ["tests.alpha", "tests.beta"],
            [.. names.OrderBy(name => name, StringComparer.Ordinal)]
        );
    }

    [Fact]
    public async Task WithoutStorage_TheStoreIsUnavailableAndReadsEmpty()
    {
        var store = new TaskRecordStore(Options.Create(new DashboardOptions()));

        Assert.False(store.IsAvailable);

        await store.RecordAsync(CreateSummary("t1", "tests.alpha", TaskState.Success));

        Assert.Null(await store.GetAsync("t1"));
        Assert.Empty(await store.GetTaskNamesAsync());
    }

    private static async Task<List<TaskSummary>> CollectAsync(IAsyncEnumerable<TaskSummary> source)
    {
        var items = new List<TaskSummary>();
        await foreach (var item in source)
        {
            items.Add(item);
        }

        return items;
    }

    private static TaskSummary CreateSummary(string taskId, string taskName, TaskState state) =>
        new()
        {
            TaskId = taskId,
            TaskName = taskName,
            State = state.ToString(),
            StartedAt = DateTimeOffset.UtcNow,
        };
}
