using DotCelery.Core.Abstractions;
using DotCelery.Core.RateLimiting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.RateLimiting;

/// <summary>
/// PostgreSQL implementation of <see cref="IRateLimiter"/>.
/// Uses sliding window algorithm with row-level locking.
/// </summary>
public sealed class PostgresRateLimiter : IRateLimiter
{
    private readonly PostgresRateLimiterOptions _options;
    private readonly ILogger<PostgresRateLimiter> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresRateLimiter"/> class.
    /// </summary>
    public PostgresRateLimiter(
        IOptions<PostgresRateLimiterOptions> options,
        ILogger<PostgresRateLimiter> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
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

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var windowStart = now.Add(-policy.Window);
        var windowEnd = now.Add(policy.Window);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            // Clean up old entries and count current window
            var cleanupSql = $"""
                DELETE FROM {_options.Schema}.{_options.TableName}
                WHERE resource_key = @resourceKey AND timestamp < @windowStart
                """;

            await using var cleanupCmd = connection.CreateCommand();
            cleanupCmd.Transaction = transaction;
            cleanupCmd.CommandText = cleanupSql;
            cleanupCmd.Parameters.AddWithValue("resourceKey", resourceKey);
            cleanupCmd.Parameters.AddWithValue("windowStart", windowStart.UtcDateTime);
            await cleanupCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Count current requests in window
            var countSql = $"""
                SELECT COUNT(*) FROM {_options.Schema}.{_options.TableName}
                WHERE resource_key = @resourceKey AND timestamp >= @windowStart
                FOR UPDATE
                """;

            await using var countCmd = connection.CreateCommand();
            countCmd.Transaction = transaction;
            countCmd.CommandText = countSql;
            countCmd.Parameters.AddWithValue("resourceKey", resourceKey);
            countCmd.Parameters.AddWithValue("windowStart", windowStart.UtcDateTime);

            var currentCount = Convert.ToInt32(
                await countCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
                System.Globalization.CultureInfo.InvariantCulture
            );

            if (currentCount >= policy.Limit)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);

                // Calculate retry after
                var oldestSql = $"""
                    SELECT MIN(timestamp) FROM {_options.Schema}.{_options.TableName}
                    WHERE resource_key = @resourceKey
                    """;

                await using var oldestCmd = _dataSource.CreateCommand(oldestSql);
                oldestCmd.Parameters.AddWithValue("resourceKey", resourceKey);

                var oldestResult = await oldestCmd
                    .ExecuteScalarAsync(cancellationToken)
                    .ConfigureAwait(false);
                var oldestTimestamp = oldestResult is DateTime dt
                    ? new DateTimeOffset(dt, TimeSpan.Zero)
                    : now;

                var retryAfter = (oldestTimestamp + policy.Window) - now;
                if (retryAfter < TimeSpan.Zero)
                {
                    retryAfter = TimeSpan.Zero;
                }

                _logger.LogDebug(
                    "Rate limit exceeded for {ResourceKey}: {Count}/{Limit}",
                    resourceKey,
                    currentCount,
                    policy.Limit
                );

                return RateLimitLease.RateLimited(retryAfter, now.Add(policy.Window));
            }

            // Add new entry
            var insertSql = $"""
                INSERT INTO {_options.Schema}.{_options.TableName}
                    (resource_key, timestamp)
                VALUES
                    (@resourceKey, @timestamp)
                """;

            await using var insertCmd = connection.CreateCommand();
            insertCmd.Transaction = transaction;
            insertCmd.CommandText = insertSql;
            insertCmd.Parameters.AddWithValue("resourceKey", resourceKey);
            insertCmd.Parameters.AddWithValue("timestamp", now.UtcDateTime);
            await insertCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            var remaining = policy.Limit - currentCount - 1;
            return RateLimitLease.Acquired(remaining, windowEnd);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
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

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var windowStart = now.Add(-policy.Window);

        var sql = $"""
            SELECT COUNT(*), MIN(timestamp) FROM {_options.Schema}.{_options.TableName}
            WHERE resource_key = @resourceKey AND timestamp >= @windowStart
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("resourceKey", resourceKey);
        cmd.Parameters.AddWithValue("windowStart", windowStart.UtcDateTime);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var count = reader.GetInt64(0);
            if (count < policy.Limit)
            {
                return null; // Permit available
            }

            if (!reader.IsDBNull(1))
            {
                var oldest = new DateTimeOffset(reader.GetDateTime(1), TimeSpan.Zero);
                var retryAfter = (oldest + policy.Window) - now;
                return retryAfter > TimeSpan.Zero ? retryAfter : TimeSpan.Zero;
            }
        }

        return null;
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

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var windowStart = now.Add(-policy.Window);

        var sql = $"""
            SELECT COUNT(*) FROM {_options.Schema}.{_options.TableName}
            WHERE resource_key = @resourceKey AND timestamp >= @windowStart
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("resourceKey", resourceKey);
        cmd.Parameters.AddWithValue("windowStart", windowStart.UtcDateTime);

        var count = Convert.ToInt32(
            await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false),
            System.Globalization.CultureInfo.InvariantCulture
        );

        return new RateLimitUsage
        {
            Used = count,
            Limit = policy.Limit,
            ResetAt = now.Add(policy.Window),
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
        await _dataSource.DisposeAsync().ConfigureAwait(false);
        _logger.LogInformation("PostgreSQL rate limiter disposed");
    }

    private async ValueTask EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        if (_options.AutoCreateTables)
        {
            await CreateTablesAsync(cancellationToken).ConfigureAwait(false);
        }

        _initialized = true;
    }

    private async Task CreateTablesAsync(CancellationToken cancellationToken)
    {
        var sql = $"""
            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.TableName} (
                id BIGSERIAL PRIMARY KEY,
                resource_key VARCHAR(255) NOT NULL,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_resource_timestamp
                ON {_options.Schema}.{_options.TableName} (resource_key, timestamp);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL rate limiter table created/verified");
    }
}
