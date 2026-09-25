using DotCelery.Core.RateLimiting;
using DotCelery.Core.Storage.Stores;

namespace DotCelery.Tests.Conformance.Stores;

/// <summary>
/// Conformance tests for <see cref="WindowRateLimiter"/>.
/// </summary>
public abstract class WindowRateLimiterConformanceTests : StoreConformanceTests
{
    private static readonly RateLimitPolicy Policy = RateLimitPolicy.PerMinute(3);

    // Each limiter instance stands for a worker in its own process
    private WindowRateLimiter CreateLimiter() => new(Provider, CreateOptions(), Time);

    [Fact]
    public async Task TryAcquireAsync_AllowsTheLimitPerWindowAcrossWorkers()
    {
        var remaining = new List<int?>();
        for (var i = 0; i < 3; i++)
        {
            var lease = await CreateLimiter().TryAcquireAsync("api", Policy);
            Assert.True(lease.IsAcquired);
            remaining.Add(lease.Remaining);
            Time.Advance(TimeSpan.FromSeconds(10));
        }

        var limited = await CreateLimiter().TryAcquireAsync("api", Policy);

        Assert.Equal([2, 1, 0], remaining);
        Assert.False(limited.IsAcquired);
        Assert.Equal(TimeSpan.FromSeconds(30), limited.RetryAfter);
        Assert.Equal(Start.AddMinutes(1), limited.ResetAt);
    }

    [Fact]
    public async Task TryAcquireAsync_AfterTheOldestUseLeavesTheWindow_AllowsAgain()
    {
        var limiter = CreateLimiter();
        for (var i = 0; i < 3; i++)
        {
            await limiter.TryAcquireAsync("api", Policy);
        }

        Time.Advance(TimeSpan.FromMinutes(1));

        Assert.True((await limiter.TryAcquireAsync("api", Policy)).IsAcquired);
    }

    [Fact]
    public async Task TryAcquireAsync_ResourcesAreIndependent()
    {
        var limiter = CreateLimiter();
        for (var i = 0; i < 3; i++)
        {
            await limiter.TryAcquireAsync("api", Policy);
        }

        Assert.True((await limiter.TryAcquireAsync("other", Policy)).IsAcquired);
    }

    [Fact]
    public async Task TryAcquireAsync_ConcurrentWorkers_NeverExceedTheLimit()
    {
        var leases = await RunConcurrentlyAsync(
            10,
            async _ => await CreateLimiter().TryAcquireAsync("api", Policy)
        );

        Assert.Equal(3, leases.Count(l => l.IsAcquired));
    }

    [Fact]
    public async Task GetRetryAfterAsync_IsSetOnlyAtTheLimit()
    {
        var limiter = CreateLimiter();
        await limiter.TryAcquireAsync("api", Policy);
        Time.Advance(TimeSpan.FromSeconds(10));
        await limiter.TryAcquireAsync("api", Policy);

        Assert.Null(await limiter.GetRetryAfterAsync("api", Policy));
        await limiter.TryAcquireAsync("api", Policy);
        Assert.Equal(TimeSpan.FromSeconds(50), await limiter.GetRetryAfterAsync("api", Policy));
    }

    [Fact]
    public async Task GetUsageAsync_ReportsUsesInTheWindow()
    {
        var limiter = CreateLimiter();
        var idle = await limiter.GetUsageAsync("api", Policy);
        await limiter.TryAcquireAsync("api", Policy);
        Time.Advance(TimeSpan.FromSeconds(10));
        await limiter.TryAcquireAsync("api", Policy);

        var usage = await limiter.GetUsageAsync("api", Policy);

        Assert.Equal(0, idle.Used);
        Assert.Equal(2, usage.Used);
        Assert.Equal(3, usage.Limit);
        Assert.Equal(Start.AddMinutes(1), usage.ResetAt);
    }
}
