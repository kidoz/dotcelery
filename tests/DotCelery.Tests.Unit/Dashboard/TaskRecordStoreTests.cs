using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Services;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

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
    public async Task Tasks_AreFoundByNameBeyondTheNameScanWindow()
    {
        var older = DateTimeOffset.UtcNow.AddHours(-1);
        await _store.RecordAsync(
            CreateSummary("t1", "tests.alpha", TaskState.Success) with
            {
                StartedAt = older,
                CompletedAt = older,
            }
        );

        // More records of another name than the names list scans: the record of the first name is
        // older than all of them, so a scan of the newest records would miss it
        for (var index = 0; index < 1000; index++)
        {
            await _store.RecordAsync(
                CreateSummary($"other-{index}", "tests.other", TaskState.Success)
            );
        }

        var found = await CollectAsync(_store.QueryByNameAsync("tests.alpha", 50, 0));

        Assert.Equal("t1", Assert.Single(found).TaskId);
    }

    [Fact]
    public async Task Tasks_ArePagedByNameNewestFirst()
    {
        var now = DateTimeOffset.UtcNow;
        for (var index = 0; index < 3; index++)
        {
            await _store.RecordAsync(
                CreateSummary($"t{index}", "tests.alpha", TaskState.Success) with
                {
                    CompletedAt = now.AddMinutes(index),
                }
            );
        }

        await _store.RecordAsync(
            CreateSummary("other", "tests.beta", TaskState.Success) with
            {
                CompletedAt = now,
            }
        );

        var firstPage = await CollectAsync(_store.QueryByNameAsync("tests.alpha", 2, 0));
        Assert.Equal(["t2", "t1"], firstPage.Select(summary => summary.TaskId));

        var secondPage = await CollectAsync(_store.QueryByNameAsync("tests.alpha", 2, 2));
        Assert.Equal(["t0"], secondPage.Select(summary => summary.TaskId));
    }

    [Fact]
    public async Task ATaskThatChangesState_IsFoundByNameOnce()
    {
        await _store.RecordAsync(CreateSummary("t1", "tests.alpha", TaskState.Started));
        await _store.RecordAsync(
            CreateSummary("t1", "tests.alpha", TaskState.Success) with
            {
                CompletedAt = DateTimeOffset.UtcNow,
            }
        );

        var found = await CollectAsync(_store.QueryByNameAsync("tests.alpha", 50, 0));

        Assert.Equal(TaskState.Success.ToString(), Assert.Single(found).State);
    }

    [Fact]
    public async Task ANameEntry_ExpiresWithItsRecord()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new TaskRecordStore(
            Options.Create(new DashboardOptions { TaskRecordRetention = TimeSpan.FromMinutes(10) }),
            new InMemoryStorageProvider(time)
        );
        await store.RecordAsync(CreateSummary("t1", "tests.alpha", TaskState.Success));
        Assert.Single(await CollectAsync(store.QueryByNameAsync("tests.alpha", 50, 0)));

        time.Advance(TimeSpan.FromMinutes(11));

        Assert.Empty(await CollectAsync(store.QueryByNameAsync("tests.alpha", 50, 0)));
        Assert.Empty(await CollectAsync(store.QueryByStateAsync(TaskState.Success, 50, 0)));
    }

    [Fact]
    public async Task TaskNames_IncludeANameBeyondTheNameScanWindow()
    {
        var older = DateTimeOffset.UtcNow.AddHours(-1);
        await _store.RecordAsync(
            CreateSummary("t1", "tests.alpha", TaskState.Success) with
            {
                StartedAt = older,
                CompletedAt = older,
            }
        );

        // More records of another name than a scan of the newest records would cover
        for (var index = 0; index < 1000; index++)
        {
            await _store.RecordAsync(
                CreateSummary($"other-{index}", "tests.other", TaskState.Success)
            );
        }

        var names = await _store.GetTaskNamesAsync();

        Assert.Equal(["tests.alpha", "tests.other"], names);
    }

    [Fact]
    public async Task TaskNames_ExpireWithTheRecords()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new TaskRecordStore(
            Options.Create(new DashboardOptions { TaskRecordRetention = TimeSpan.FromMinutes(10) }),
            new InMemoryStorageProvider(time)
        );
        await store.RecordAsync(CreateSummary("t1", "tests.alpha", TaskState.Success));
        Assert.Equal(["tests.alpha"], await store.GetTaskNamesAsync());

        time.Advance(TimeSpan.FromMinutes(11));

        Assert.Empty(await store.GetTaskNamesAsync());
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
