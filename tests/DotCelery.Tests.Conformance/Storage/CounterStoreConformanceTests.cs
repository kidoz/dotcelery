using DotCelery.Core.Storage;

namespace DotCelery.Tests.Conformance.Storage;

/// <summary>
/// Conformance tests for <see cref="ICounterStore"/>.
/// </summary>
public abstract class CounterStoreConformanceTests : StorageConformanceTests
{
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    private readonly string _key = Unique("counter");

    private ICounterStore Counters => Provider.Counters;

    [Fact]
    public async Task IncrementAsync_ReturnsTheRunningTotal()
    {
        Assert.Equal(1, await Counters.IncrementAsync(_key));
        Assert.Equal(6, await Counters.IncrementAsync(_key, 5));
        Assert.Equal(4, await Counters.IncrementAsync(_key, -2));
        Assert.Equal(4, await Counters.GetAsync(_key));
    }

    [Fact]
    public async Task GetAsync_MissingCounter_ReturnsZero()
    {
        Assert.Equal(0, await Counters.GetAsync(_key));
    }

    [Fact]
    public async Task IncrementAsync_WithTimeToLive_ExpiresFromCreation()
    {
        await Counters.IncrementAsync(_key, 1, TimeSpan.FromMinutes(1));
        Time.Advance(TimeSpan.FromSeconds(30));

        // A later increment does not extend the expiry
        Assert.Equal(2, await Counters.IncrementAsync(_key, 1, TimeSpan.FromMinutes(1)));

        Time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(0, await Counters.GetAsync(_key));
        Assert.Equal(1, await Counters.IncrementAsync(_key));
    }

    [Fact]
    public async Task DeleteAsync_RemovesTheCounter()
    {
        await Counters.IncrementAsync(_key, 3);

        Assert.True(await Counters.DeleteAsync(_key));
        Assert.Equal(0, await Counters.GetAsync(_key));
        Assert.False(await Counters.DeleteAsync(_key));
    }

    [Fact]
    public async Task TryAddToWindowAsync_AllowsUpToTheLimit()
    {
        Assert.Equal(new WindowResult(true, 1, null), await Add(3));
        Time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(new WindowResult(true, 2, null), await Add(3));
        Time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(new WindowResult(true, 3, null), await Add(3));

        Assert.Equal(new WindowResult(false, 3, TimeSpan.FromSeconds(40)), await Add(3));
    }

    [Fact]
    public async Task TryAddToWindowAsync_EventsLeaveTheWindowWhenItElapses()
    {
        await Add(1);

        Time.Advance(TimeSpan.FromSeconds(59));
        Assert.Equal(new WindowResult(false, 1, TimeSpan.FromSeconds(1)), await Add(1));

        Time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(new WindowResult(true, 1, null), await Add(1));
    }

    [Fact]
    public async Task GetWindowAsync_ReturnsEventsWithinTheWindow()
    {
        await Add(10);
        Time.Advance(TimeSpan.FromSeconds(30));
        await Add(10);
        Time.Advance(TimeSpan.FromSeconds(15));
        await Add(10);
        Time.Advance(TimeSpan.FromSeconds(15));

        Assert.Equal(
            new WindowSnapshot(2, Start.AddSeconds(30)),
            await Counters.GetWindowAsync(_key, Window)
        );
        Assert.Equal(
            new WindowSnapshot(1, Start.AddSeconds(45)),
            await Counters.GetWindowAsync(_key, TimeSpan.FromSeconds(20))
        );
        Assert.Equal(
            new WindowSnapshot(0, null),
            await Counters.GetWindowAsync(Unique("counter"), Window)
        );
    }

    [Fact]
    public async Task IncrementAsync_Concurrent_AddsEveryIncrement()
    {
        await RunConcurrentlyAsync(50, async _ => await Counters.IncrementAsync(_key));

        Assert.Equal(50, await Counters.GetAsync(_key));
    }

    [Fact]
    public async Task TryAddToWindowAsync_Concurrent_NeverExceedsTheLimit()
    {
        var results = await RunConcurrentlyAsync(50, async _ => await Add(10));

        Assert.Equal(10, results.Count(r => r.Added));
        Assert.Equal(10, (await Counters.GetWindowAsync(_key, Window)).Count);
    }

    private ValueTask<WindowResult> Add(int limit) =>
        Counters.TryAddToWindowAsync(_key, limit, Window);
}
