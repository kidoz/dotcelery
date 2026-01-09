using DotCelery.Backend.Redis.RateLimiting;
using DotCelery.Core.RateLimiting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Redis;

namespace DotCelery.Tests.Integration.Redis;

/// <summary>
/// Integration tests for Redis rate limiter using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Redis")]
public class RedisRateLimiterIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _container;
    private RedisRateLimiter? _rateLimiter;

    public RedisRateLimiterIntegrationTests()
    {
        _container = new RedisBuilder("redis:7-alpine").Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new RedisRateLimiterOptions { ConnectionString = _container.GetConnectionString() }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisRateLimiter>();

        _rateLimiter = new RedisRateLimiter(options, logger);
    }

    public async ValueTask DisposeAsync()
    {
        if (_rateLimiter is not null)
        {
            await _rateLimiter.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    [Fact]
    public async Task TryAcquireAsync_UnderLimit_ReturnsAcquired()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(10);

        var lease = await _rateLimiter!.TryAcquireAsync(resourceKey, policy);

        Assert.True(lease.IsAcquired);
        Assert.Equal(9, lease.Remaining);
    }

    [Fact]
    public async Task TryAcquireAsync_ExceedsLimit_ReturnsRateLimited()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(3);

        // Acquire 3 times (the limit)
        for (int i = 0; i < 3; i++)
        {
            var lease = await _rateLimiter!.TryAcquireAsync(resourceKey, policy);
            Assert.True(lease.IsAcquired);
        }

        // 4th acquire should fail
        var failedLease = await _rateLimiter!.TryAcquireAsync(resourceKey, policy);

        Assert.False(failedLease.IsAcquired);
        Assert.True(failedLease.RetryAfter > TimeSpan.Zero);
    }

    [Fact]
    public async Task TryAcquireAsync_RemainingDecreases()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(5);

        var lease1 = await _rateLimiter!.TryAcquireAsync(resourceKey, policy);
        var lease2 = await _rateLimiter.TryAcquireAsync(resourceKey, policy);
        var lease3 = await _rateLimiter.TryAcquireAsync(resourceKey, policy);

        Assert.Equal(4, lease1.Remaining);
        Assert.Equal(3, lease2.Remaining);
        Assert.Equal(2, lease3.Remaining);
    }

    [Fact]
    public async Task TryAcquireAsync_SlidingWindowResetsAfterWindow()
    {
        var resourceKey = Guid.NewGuid().ToString();
        // Very short window for testing
        var policy = new RateLimitPolicy { Limit = 2, Window = TimeSpan.FromMilliseconds(200) };

        // Use up the limit
        await _rateLimiter!.TryAcquireAsync(resourceKey, policy);
        await _rateLimiter.TryAcquireAsync(resourceKey, policy);

        var limitedLease = await _rateLimiter.TryAcquireAsync(resourceKey, policy);
        Assert.False(limitedLease.IsAcquired);

        // Wait for window to pass
        await Task.Delay(300);

        // Should be able to acquire again
        var newLease = await _rateLimiter.TryAcquireAsync(resourceKey, policy);
        Assert.True(newLease.IsAcquired);
    }

    [Fact]
    public async Task GetRetryAfterAsync_UnderLimit_ReturnsNull()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(10);

        var retryAfter = await _rateLimiter!.GetRetryAfterAsync(resourceKey, policy);

        Assert.Null(retryAfter);
    }

    [Fact]
    public async Task GetRetryAfterAsync_AtLimit_ReturnsPositiveValue()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(2);

        // Use up the limit
        await _rateLimiter!.TryAcquireAsync(resourceKey, policy);
        await _rateLimiter.TryAcquireAsync(resourceKey, policy);

        var retryAfter = await _rateLimiter.GetRetryAfterAsync(resourceKey, policy);

        Assert.NotNull(retryAfter);
        Assert.True(retryAfter.Value > TimeSpan.Zero);
    }

    [Fact]
    public async Task GetUsageAsync_ReturnsCorrectUsage()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(10);

        // Make some requests
        await _rateLimiter!.TryAcquireAsync(resourceKey, policy);
        await _rateLimiter.TryAcquireAsync(resourceKey, policy);
        await _rateLimiter.TryAcquireAsync(resourceKey, policy);

        var usage = await _rateLimiter.GetUsageAsync(resourceKey, policy);

        Assert.Equal(3, usage.Used);
        Assert.Equal(10, usage.Limit);
    }

    [Fact]
    public async Task GetUsageAsync_NewResource_ReturnsZeroUsed()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(10);

        var usage = await _rateLimiter!.GetUsageAsync(resourceKey, policy);

        Assert.Equal(0, usage.Used);
        Assert.Equal(10, usage.Limit);
    }

    [Fact]
    public async Task TryAcquireAsync_DifferentResources_IndependentLimits()
    {
        var resource1 = Guid.NewGuid().ToString();
        var resource2 = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(2);

        // Use up limit on resource1
        await _rateLimiter!.TryAcquireAsync(resource1, policy);
        await _rateLimiter.TryAcquireAsync(resource1, policy);
        var limitedLease = await _rateLimiter.TryAcquireAsync(resource1, policy);
        Assert.False(limitedLease.IsAcquired);

        // resource2 should still be available
        var lease2 = await _rateLimiter.TryAcquireAsync(resource2, policy);
        Assert.True(lease2.IsAcquired);
    }

    [Fact]
    public async Task TryAcquireAsync_DifferentPolicies_IndependentLimits()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policyPerSecond = RateLimitPolicy.PerSecond(2);
        var policyPerMinute = RateLimitPolicy.PerMinute(100);

        // Use up the per-second limit
        await _rateLimiter!.TryAcquireAsync(resourceKey, policyPerSecond);
        await _rateLimiter.TryAcquireAsync(resourceKey, policyPerSecond);
        var limitedLease = await _rateLimiter.TryAcquireAsync(resourceKey, policyPerSecond);
        Assert.False(limitedLease.IsAcquired);

        // Per-minute limit should still be available
        var minuteLease = await _rateLimiter.TryAcquireAsync(resourceKey, policyPerMinute);
        Assert.True(minuteLease.IsAcquired);
    }

    [Fact]
    public async Task ConcurrentAcquires_DoNotExceedLimit()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(10);

        var tasks = Enumerable
            .Range(0, 20)
            .Select(_ => _rateLimiter!.TryAcquireAsync(resourceKey, policy).AsTask());

        var leases = await Task.WhenAll(tasks);

        var acquiredCount = leases.Count(l => l.IsAcquired);
        Assert.Equal(10, acquiredCount);
    }

    [Fact]
    public async Task TryAcquireAsync_ResetAtIsInFuture()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(5);

        var lease = await _rateLimiter!.TryAcquireAsync(resourceKey, policy);

        Assert.True(lease.ResetAt > DateTimeOffset.UtcNow);
        Assert.True(lease.ResetAt < DateTimeOffset.UtcNow.AddSeconds(2));
    }

    [Fact]
    public async Task TwoRateLimiters_ShareState()
    {
        // Create a second rate limiter pointing to the same Redis
        var options = Options.Create(
            new RedisRateLimiterOptions { ConnectionString = _container.GetConnectionString() }
        );
        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisRateLimiter>();
        await using var rateLimiter2 = new RedisRateLimiter(options, logger);

        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.PerSecond(3);

        // Use rate limiter 1
        await _rateLimiter!.TryAcquireAsync(resourceKey, policy);
        await _rateLimiter.TryAcquireAsync(resourceKey, policy);

        // Rate limiter 2 should see the same state
        var lease = await rateLimiter2.TryAcquireAsync(resourceKey, policy);
        Assert.True(lease.IsAcquired);
        Assert.Equal(0, lease.Remaining); // 2 from limiter1 + 1 from limiter2 = 3 total (limit)

        // Both limiters should now be limited
        var limited1 = await _rateLimiter.TryAcquireAsync(resourceKey, policy);
        var limited2 = await rateLimiter2.TryAcquireAsync(resourceKey, policy);
        Assert.False(limited1.IsAcquired);
        Assert.False(limited2.IsAcquired);
    }

    [Fact]
    public async Task TryAcquireAsync_ParsedPolicy_Works()
    {
        var resourceKey = Guid.NewGuid().ToString();
        var policy = RateLimitPolicy.Parse("5/s");

        var lease = await _rateLimiter!.TryAcquireAsync(resourceKey, policy);

        Assert.True(lease.IsAcquired);
        Assert.Equal(4, lease.Remaining);
    }
}
