using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="PartitionLockStore"/> and <see cref="TaskExecutionTracker"/>.
/// </summary>
public abstract class LeaseBackedStoreConformanceTests : StoreConformanceTests
{
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(30);

    private PartitionLockStore CreateLocks() => new(Provider, CreateOptions());

    private TaskExecutionTracker CreateTracker() => new(Provider, CreateOptions());

    [Fact]
    public async Task PartitionLock_IsHeldByOneTaskAtATime()
    {
        var locks = CreateLocks();

        Assert.True(await locks.TryAcquireAsync("p", "task-1", Duration));
        Assert.True(await locks.TryAcquireAsync("p", "task-1", Duration));
        Assert.False(await CreateLocks().TryAcquireAsync("p", "task-2", Duration));
        Assert.True(await locks.TryAcquireAsync("other", "task-2", Duration));

        Assert.True(await locks.IsLockedAsync("p"));
        Assert.Equal("task-1", await locks.GetLockHolderAsync("p"));
        Assert.False(await locks.IsLockedAsync("free"));
        Assert.Null(await locks.GetLockHolderAsync("free"));
    }

    [Fact]
    public async Task PartitionLock_ReleaseAsync_OnlyReleasesTheHoldersLock()
    {
        var locks = CreateLocks();
        await locks.TryAcquireAsync("p", "task-1", Duration);

        Assert.False(await locks.ReleaseAsync("p", "task-2"));
        Assert.True(await locks.ReleaseAsync("p", "task-1"));
        Assert.False(await locks.ReleaseAsync("p", "task-1"));
        Assert.True(await locks.TryAcquireAsync("p", "task-2", Duration));
    }

    [Fact]
    public async Task PartitionLock_Expired_CanBeTakenOverAndNotReleasedByTheOldHolder()
    {
        var locks = CreateLocks();
        await locks.TryAcquireAsync("p", "task-1", Duration);

        Time.Advance(Duration);
        Assert.False(await locks.IsLockedAsync("p"));
        Assert.True(await locks.TryAcquireAsync("p", "task-2", Duration));

        Assert.False(await locks.ReleaseAsync("p", "task-1"));
        Assert.False(await locks.ExtendAsync("p", "task-1", Duration));
        Assert.Equal("task-2", await locks.GetLockHolderAsync("p"));
    }

    [Fact]
    public async Task PartitionLock_ExtendAsync_KeepsTheLockFromNow()
    {
        var locks = CreateLocks();
        await locks.TryAcquireAsync("p", "task-1", Duration);

        Time.Advance(TimeSpan.FromSeconds(20));
        Assert.True(await locks.ExtendAsync("p", "task-1", Duration));
        Assert.False(await locks.ExtendAsync("p", "task-2", Duration));

        Time.Advance(TimeSpan.FromSeconds(29));
        Assert.Equal("task-1", await locks.GetLockHolderAsync("p"));
        Time.Advance(TimeSpan.FromSeconds(1));
        Assert.False(await locks.IsLockedAsync("p"));
    }

    [Fact]
    public async Task PartitionLock_ConcurrentTasks_OnlyOneAcquires()
    {
        var results = await RunConcurrentlyAsync(
            8,
            async i => await CreateLocks().TryAcquireAsync("p", $"task-{i}", Duration)
        );

        Assert.Single(results, acquired => acquired);
    }

    [Fact]
    public async Task ExecutionTracker_AllowsOneExecutionPerTaskAndKey()
    {
        var tracker = CreateTracker();

        Assert.True(await tracker.TryStartAsync("tests.task", "task-1"));
        Assert.False(await CreateTracker().TryStartAsync("tests.task", "task-2"));
        Assert.True(await tracker.TryStartAsync("tests.task", "task-3", "tenant-a"));
        Assert.False(await tracker.TryStartAsync("tests.task", "task-4", "tenant-a"));
        Assert.True(await tracker.TryStartAsync("tests.other", "task-5"));

        Assert.True(await tracker.IsExecutingAsync("tests.task"));
        Assert.True(await tracker.IsExecutingAsync("tests.task", "tenant-a"));
        Assert.False(await tracker.IsExecutingAsync("tests.task", "tenant-b"));
        Assert.Equal("task-3", await tracker.GetExecutingTaskIdAsync("tests.task", "tenant-a"));
        Assert.Null(await tracker.GetExecutingTaskIdAsync("tests.idle"));
    }

    [Fact]
    public async Task ExecutionTracker_KeysWithSeparators_AreDistinct()
    {
        var tracker = CreateTracker();

        Assert.True(await tracker.TryStartAsync("tests/task", "task-1"));
        Assert.True(await tracker.TryStartAsync("tests", "task-2", "task"));
        Assert.True(await tracker.TryStartAsync("tests", "task-3", "a/b:c"));

        Assert.Equal("task-1", await tracker.GetExecutingTaskIdAsync("tests/task"));
        Assert.Equal("task-3", await tracker.GetExecutingTaskIdAsync("tests", "a/b:c"));
    }

    [Fact]
    public async Task ExecutionTracker_StopAsync_OnlyStopsTheRunningTask()
    {
        var tracker = CreateTracker();
        await tracker.TryStartAsync("tests.task", "task-1");

        await tracker.StopAsync("tests.task", "task-2");
        Assert.True(await tracker.IsExecutingAsync("tests.task"));

        await tracker.StopAsync("tests.task", "task-1");
        Assert.False(await tracker.IsExecutingAsync("tests.task"));
        Assert.True(await tracker.TryStartAsync("tests.task", "task-2"));
    }

    [Fact]
    public async Task ExecutionTracker_Timeout_EndsTheExecutionUnlessExtended()
    {
        var tracker = CreateTracker();
        await tracker.TryStartAsync("tests.task", "task-1", timeout: Duration);
        await tracker.TryStartAsync("tests.other", "task-2", timeout: Duration);

        Time.Advance(TimeSpan.FromSeconds(20));
        Assert.True(await tracker.ExtendAsync("tests.task", "task-1", extension: Duration));
        Assert.False(await tracker.ExtendAsync("tests.task", "task-2", extension: Duration));

        Time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(await tracker.IsExecutingAsync("tests.task"));
        Assert.False(await tracker.IsExecutingAsync("tests.other"));
        Assert.True(await tracker.TryStartAsync("tests.other", "task-3"));
    }

    [Fact]
    public async Task ExecutionTracker_GetAllExecutingAsync_ListsOnlyItsOwnExecutions()
    {
        var tracker = CreateTracker();
        await tracker.TryStartAsync("tests.task", "task-1", timeout: Duration);
        Time.Advance(TimeSpan.FromSeconds(5));
        await tracker.TryStartAsync("tests.task", "task-2", "a/b", Duration);
        await new TaskExecutionTracker(
            Provider,
            Microsoft.Extensions.Options.Options.Create(
                new StorageStoreOptions { Prefix = Unique("other") }
            )
        ).TryStartAsync("tests.task", "task-3");

        var executing = await tracker.GetAllExecutingAsync();

        Assert.Equal(["tests.task", "tests.task:a/b"], executing.Keys.Order());
        var first = executing["tests.task"];
        Assert.Equal("task-1", first.TaskId);
        Assert.Null(first.Key);
        Assert.Equal(Start, first.StartedAt);
        Assert.Equal(Start + Duration, first.ExpiresAt);
        Assert.Equal("a/b", executing["tests.task:a/b"].Key);
        Assert.Equal(Start.AddSeconds(5), executing["tests.task:a/b"].StartedAt);
    }
}
