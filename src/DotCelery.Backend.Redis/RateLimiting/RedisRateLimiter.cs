using DotCelery.Core.Abstractions;
using DotCelery.Core.RateLimiting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.RateLimiting;

/// <summary>
/// Redis implementation of rate limiter using sorted sets for sliding window algorithm.
/// Provides distributed rate limiting across multiple workers.
/// </summary>
public sealed class RedisRateLimiter : IRateLimiter
{
    private readonly RedisRateLimiterOptions _options;
    private readonly ILogger<RedisRateLimiter> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    // Lua script for atomic sliding window rate limit check and acquire
    // Returns: [isAcquired (0/1), remaining, retryAfterMs, resetAtMs]
    private const string SlidingWindowScript = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])
        local requestId = ARGV[4]

        local windowStart = now - window

        -- Remove expired entries
        redis.call('ZREMRANGEBYSCORE', key, '-inf', windowStart)

        -- Count current entries
        local count = redis.call('ZCARD', key)

        if count >= limit then
            -- Rate limited - get oldest entry to calculate retry after
            local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
            local retryAfter = 0
            local resetAt = now + window

            if #oldest >= 2 then
                local oldestTime = tonumber(oldest[2])
                retryAfter = (oldestTime + window) - now
                resetAt = oldestTime + window
            end

            if retryAfter < 1 then
                retryAfter = 1
            end

            return {0, 0, retryAfter, resetAt}
        end

        -- Add new entry
        redis.call('ZADD', key, now, requestId)

        -- Set expiry on the key
        redis.call('PEXPIRE', key, window + 1000)

        -- Calculate remaining and reset time
        local newCount = count + 1
        local remaining = limit - newCount

        local oldestAfterAdd = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
        local resetAt = now + window

        if #oldestAfterAdd >= 2 then
            resetAt = tonumber(oldestAfterAdd[2]) + window
        end

        return {1, remaining, 0, resetAt}
        """;

    // Lua script for checking rate limit status without acquiring
    private const string CheckStatusScript = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])

        local windowStart = now - window

        -- Remove expired entries
        redis.call('ZREMRANGEBYSCORE', key, '-inf', windowStart)

        -- Count current entries
        local count = redis.call('ZCARD', key)

        local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
        local resetAt = now + window

        if #oldest >= 2 then
            resetAt = tonumber(oldest[2]) + window
        end

        if count >= limit then
            local retryAfter = resetAt - now
            if retryAfter < 1 then
                retryAfter = 1
            end
            return {count, limit, retryAfter, resetAt}
        end

        return {count, limit, 0, resetAt}
        """;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisRateLimiter"/> class.
    /// </summary>
    /// <param name="options">The rate limiter options.</param>
    /// <param name="logger">The logger.</param>
    public RedisRateLimiter(
        IOptions<RedisRateLimiterOptions> options,
        ILogger<RedisRateLimiter> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitLease> TryAcquireAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetKey(resourceKey, policy);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var windowMs = (long)policy.Window.TotalMilliseconds;
        var requestId = $"{now}:{Guid.NewGuid():N}";

        var result = await db.ScriptEvaluateAsync(
                SlidingWindowScript,
                [key],
                [now, windowMs, policy.Limit, requestId]
            )
            .ConfigureAwait(false);

        var values = (long[])result!;
        var isAcquired = values[0] == 1;
        var remaining = (int)values[1];
        var retryAfterMs = values[2];
        var resetAtMs = values[3];

        var resetAt = DateTimeOffset.FromUnixTimeMilliseconds(resetAtMs);

        if (isAcquired)
        {
            _logger.LogDebug(
                "Rate limit acquired for {ResourceKey}: {Remaining} remaining, resets at {ResetAt}",
                resourceKey,
                remaining,
                resetAt
            );

            return RateLimitLease.Acquired(remaining, resetAt);
        }

        var retryAfter = TimeSpan.FromMilliseconds(retryAfterMs);

        _logger.LogDebug(
            "Rate limit exceeded for {ResourceKey}: retry after {RetryAfter}, resets at {ResetAt}",
            resourceKey,
            retryAfter,
            resetAt
        );

        return RateLimitLease.RateLimited(retryAfter, resetAt);
    }

    /// <inheritdoc />
    public async ValueTask<TimeSpan?> GetRetryAfterAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetKey(resourceKey, policy);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var windowMs = (long)policy.Window.TotalMilliseconds;

        var result = await db.ScriptEvaluateAsync(
                CheckStatusScript,
                [key],
                [now, windowMs, policy.Limit]
            )
            .ConfigureAwait(false);

        var values = (long[])result!;
        var retryAfterMs = values[2];

        if (retryAfterMs <= 0)
        {
            return null;
        }

        return TimeSpan.FromMilliseconds(retryAfterMs);
    }

    /// <inheritdoc />
    public async ValueTask<RateLimitUsage> GetUsageAsync(
        string resourceKey,
        RateLimitPolicy policy,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(resourceKey);
        ArgumentNullException.ThrowIfNull(policy);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetKey(resourceKey, policy);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var windowMs = (long)policy.Window.TotalMilliseconds;

        var result = await db.ScriptEvaluateAsync(
                CheckStatusScript,
                [key],
                [now, windowMs, policy.Limit]
            )
            .ConfigureAwait(false);

        var values = (long[])result!;
        var used = (int)values[0];
        var limit = (int)values[1];
        var resetAtMs = values[3];

        return new RateLimitUsage
        {
            Used = used,
            Limit = limit,
            ResetAt = DateTimeOffset.FromUnixTimeMilliseconds(resetAtMs),
        };
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection.Dispose();
        }

        _connectionLock.Dispose();

        _logger.LogInformation("Redis rate limiter disposed");
    }

    private string GetKey(string resourceKey, RateLimitPolicy policy) =>
        $"{_options.KeyPrefix}{resourceKey}:{policy.Window.TotalSeconds}:{policy.Limit}";

    private async Task<IConnectionMultiplexer> GetConnectionAsync(
        CancellationToken cancellationToken
    )
    {
        if (_connection?.IsConnected == true)
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection?.IsConnected == true)
            {
                return _connection;
            }

            var configOptions = ConfigurationOptions.Parse(_options.ConnectionString);
            configOptions.DefaultDatabase = _options.Database;
            configOptions.ConnectTimeout = (int)_options.ConnectTimeout.TotalMilliseconds;
            configOptions.SyncTimeout = (int)_options.SyncTimeout.TotalMilliseconds;
            configOptions.AbortOnConnectFail = _options.AbortOnConnectFail;

            _connection = await ConnectionMultiplexer
                .ConnectAsync(configOptions)
                .ConfigureAwait(false);
            _logger.LogInformation(
                "Connected to Redis for rate limiter at {ConnectionString}",
                _options.ConnectionString
            );

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return connection.GetDatabase(_options.Database);
    }
}
