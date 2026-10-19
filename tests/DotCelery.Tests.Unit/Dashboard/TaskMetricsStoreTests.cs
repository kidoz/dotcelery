using DotCelery.Backend.InMemory.Storage;
using DotCelery.Dashboard.Services;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Exercises the per-task totals the dashboard's collector diffs into the history: they are
/// counted per task name, split by outcome, and list the names that have totals.
/// </summary>
public sealed class TaskMetricsStoreTests
{
    private readonly InMemoryStorageProvider _storage = new();
    private readonly TaskMetricsStore _store;

    public TaskMetricsStoreTests()
    {
        _store = new TaskMetricsStore(_storage);
    }

    [Fact]
    public async Task Totals_AreCountedPerTaskNameAndOutcome()
    {
        await _store.RecordCompletedAsync("tests.alpha", success: true, TimeSpan.FromSeconds(2));
        await _store.RecordCompletedAsync("tests.alpha", success: false, TimeSpan.FromSeconds(4));
        await _store.RecordCompletedAsync("tests.beta", success: true, TimeSpan.FromSeconds(1));

        var totals = await _store.GetTotalsAsync();

        Assert.Equal(1, totals["tests.alpha"].Succeeded);
        Assert.Equal(1, totals["tests.alpha"].Failed);
        Assert.Equal(2, totals["tests.alpha"].Total);
        Assert.Equal(TimeSpan.FromSeconds(6).Ticks, totals["tests.alpha"].DurationTicks);
        Assert.Equal(1, totals["tests.beta"].Succeeded);
        Assert.Equal(TimeSpan.FromSeconds(1).Ticks, totals["tests.beta"].DurationTicks);
    }

    [Fact]
    public async Task ATaskNameThatContainsASlash_KeepsItsOwnTotals()
    {
        await _store.RecordCompletedAsync("tests/alpha", success: true, TimeSpan.Zero);
        await _store.RecordCompletedAsync("tests", success: true, TimeSpan.Zero);

        var totals = await _store.GetTotalsAsync();

        Assert.Equal(2, totals.Count);
        Assert.Equal(1, totals["tests/alpha"].Succeeded);
        Assert.Equal(1, totals["tests"].Succeeded);
    }

    [Fact]
    public async Task WithoutStorage_TheStoreIsUnavailableAndReportsNothing()
    {
        var store = new TaskMetricsStore();

        Assert.False(store.IsAvailable);

        await store.RecordCompletedAsync("tests.alpha", success: true, TimeSpan.Zero);

        Assert.Empty(await store.GetTotalsAsync());
    }
}
