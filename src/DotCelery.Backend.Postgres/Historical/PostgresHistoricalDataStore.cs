using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Historical;

/// <summary>
/// PostgreSQL implementation of <see cref="IHistoricalDataStore"/>.
/// </summary>
public sealed class PostgresHistoricalDataStore : IHistoricalDataStore
{
    private readonly PostgresHistoricalDataStoreOptions _options;
    private readonly ILogger<PostgresHistoricalDataStore> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly JsonSerializerOptions _jsonOptions;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresHistoricalDataStore"/> class.
    /// </summary>
    public PostgresHistoricalDataStore(
        IOptions<PostgresHistoricalDataStoreOptions> options,
        ILogger<PostgresHistoricalDataStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    /// <inheritdoc />
    public async ValueTask RecordMetricsAsync(
        MetricsSnapshot snapshot,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(snapshot);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.SnapshotsTableName}
                (id, timestamp, task_name, total_processed, success_count, failure_count,
                 retry_count, revoked_count, avg_execution_time_ms, queue)
            VALUES
                (@id, @timestamp, @taskName, @totalProcessed, @successCount, @failureCount,
                 @retryCount, @revokedCount, @avgExecutionTimeMs, @queue)
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", Guid.NewGuid().ToString("N"));
        cmd.Parameters.AddWithValue("timestamp", snapshot.Timestamp.UtcDateTime);
        cmd.Parameters.AddWithValue("taskName", snapshot.TaskName ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("totalProcessed", snapshot.TotalProcessed);
        cmd.Parameters.AddWithValue("successCount", snapshot.SuccessCount);
        cmd.Parameters.AddWithValue("failureCount", snapshot.FailureCount);
        cmd.Parameters.AddWithValue("retryCount", snapshot.RetryCount);
        cmd.Parameters.AddWithValue("revokedCount", snapshot.RevokedCount);
        cmd.Parameters.AddWithValue(
            "avgExecutionTimeMs",
            snapshot.AverageExecutionTime?.TotalMilliseconds ?? (object)DBNull.Value
        );
        cmd.Parameters.AddWithValue("queue", snapshot.Queue ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Recorded metrics snapshot at {Timestamp}", snapshot.Timestamp);
    }

    /// <inheritdoc />
    public async ValueTask<AggregatedMetrics> GetMetricsAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT
                COALESCE(SUM(success_count), 0) as total_success,
                COALESCE(SUM(failure_count), 0) as total_failure,
                COALESCE(SUM(retry_count), 0) as total_retry,
                COALESCE(SUM(revoked_count), 0) as total_revoked,
                COALESCE(SUM(total_processed), 0) as total_processed,
                AVG(avg_execution_time_ms) FILTER (WHERE avg_execution_time_ms IS NOT NULL) as avg_time
            FROM {_options.Schema}.{_options.SnapshotsTableName}
            WHERE timestamp >= @from AND timestamp < @until
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("from", from.UtcDateTime);
        cmd.Parameters.AddWithValue("until", until.UtcDateTime);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return CreateEmptyMetrics(from, until, granularity);
        }

        var totalSuccess = reader.GetInt64(0);
        var totalFailure = reader.GetInt64(1);
        var totalRetry = reader.GetInt64(2);
        var totalRevoked = reader.GetInt64(3);
        var totalProcessed = reader.GetInt64(4);
        var avgTimeMs = reader.IsDBNull(5) ? (double?)null : reader.GetDouble(5);

        var durationSeconds = (until - from).TotalSeconds;
        var tasksPerSecond = durationSeconds > 0 ? totalProcessed / durationSeconds : 0;

        return new AggregatedMetrics
        {
            From = from,
            To = until,
            Granularity = granularity,
            TotalProcessed = totalProcessed,
            SuccessCount = totalSuccess,
            FailureCount = totalFailure,
            RetryCount = totalRetry,
            RevokedCount = totalRevoked,
            AverageExecutionTime = avgTimeMs.HasValue
                ? TimeSpan.FromMilliseconds(avgTimeMs.Value)
                : null,
            TasksPerSecond = tasksPerSecond,
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<MetricsDataPoint> GetTimeSeriesAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity = MetricsGranularity.Hour,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var bucketSize = GetBucketSize(granularity);
        var truncateExpr = GetTruncateExpression(granularity);

        var sql = $"""
            SELECT
                {truncateExpr} as bucket,
                COALESCE(SUM(success_count), 0) as success,
                COALESCE(SUM(failure_count), 0) as failure,
                COALESCE(SUM(retry_count), 0) as retry,
                COALESCE(SUM(total_processed), 0) as processed,
                AVG(avg_execution_time_ms) FILTER (WHERE avg_execution_time_ms IS NOT NULL) as avg_time
            FROM {_options.Schema}.{_options.SnapshotsTableName}
            WHERE timestamp >= @from AND timestamp < @until
            GROUP BY bucket
            ORDER BY bucket
            LIMIT @limit
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("from", from.UtcDateTime);
        cmd.Parameters.AddWithValue("until", until.UtcDateTime);
        cmd.Parameters.AddWithValue("limit", _options.MaxDataPoints);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var bucket = new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero);
            var success = reader.GetInt64(1);
            var failure = reader.GetInt64(2);
            var retry = reader.GetInt64(3);
            var processed = reader.GetInt64(4);
            var avgTimeMs = reader.IsDBNull(5) ? (double?)null : reader.GetDouble(5);

            var tasksPerSecond =
                bucketSize.TotalSeconds > 0 ? processed / bucketSize.TotalSeconds : 0;

            yield return new MetricsDataPoint
            {
                Timestamp = bucket,
                SuccessCount = success,
                FailureCount = failure,
                RetryCount = retry,
                TasksPerSecond = tasksPerSecond,
                AverageExecutionTime = avgTimeMs.HasValue
                    ? TimeSpan.FromMilliseconds(avgTimeMs.Value)
                    : null,
            };
        }
    }

    /// <inheritdoc />
    public async ValueTask<
        IReadOnlyDictionary<string, TaskMetricsSummary>
    > GetMetricsByTaskNameAsync(
        DateTimeOffset from,
        DateTimeOffset until,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT
                task_name,
                COALESCE(SUM(total_processed), 0) as total,
                COALESCE(SUM(success_count), 0) as success,
                COALESCE(SUM(failure_count), 0) as failure,
                AVG(avg_execution_time_ms) FILTER (WHERE avg_execution_time_ms IS NOT NULL) as avg_time,
                MIN(avg_execution_time_ms) FILTER (WHERE avg_execution_time_ms IS NOT NULL) as min_time,
                MAX(avg_execution_time_ms) FILTER (WHERE avg_execution_time_ms IS NOT NULL) as max_time
            FROM {_options.Schema}.{_options.SnapshotsTableName}
            WHERE timestamp >= @from AND timestamp < @until AND task_name IS NOT NULL
            GROUP BY task_name
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("from", from.UtcDateTime);
        cmd.Parameters.AddWithValue("until", until.UtcDateTime);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        var result = new Dictionary<string, TaskMetricsSummary>();

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var taskName = reader.GetString(0);
            var avgTimeMs = reader.IsDBNull(4) ? (double?)null : reader.GetDouble(4);
            var minTimeMs = reader.IsDBNull(5) ? (double?)null : reader.GetDouble(5);
            var maxTimeMs = reader.IsDBNull(6) ? (double?)null : reader.GetDouble(6);

            result[taskName] = new TaskMetricsSummary
            {
                TaskName = taskName,
                TotalCount = reader.GetInt64(1),
                SuccessCount = reader.GetInt64(2),
                FailureCount = reader.GetInt64(3),
                AverageExecutionTime = avgTimeMs.HasValue
                    ? TimeSpan.FromMilliseconds(avgTimeMs.Value)
                    : null,
                MinExecutionTime = minTimeMs.HasValue
                    ? TimeSpan.FromMilliseconds(minTimeMs.Value)
                    : null,
                MaxExecutionTime = maxTimeMs.HasValue
                    ? TimeSpan.FromMilliseconds(maxTimeMs.Value)
                    : null,
            };
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask<long> ApplyRetentionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var cutoff = DateTimeOffset.UtcNow.Subtract(_options.RetentionPeriod);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.SnapshotsTableName}
            WHERE timestamp < @cutoff
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("cutoff", cutoff.UtcDateTime);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogInformation("Removed {Count} expired snapshots", deleted);
        }

        return deleted;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetSnapshotCountAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COUNT(*) FROM {_options.Schema}.{_options.SnapshotsTableName}
            """;

        await using var cmd = _dataSource.CreateCommand(sql);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
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
        _logger.LogInformation("PostgreSQL historical data store disposed");
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
            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.SnapshotsTableName} (
                id VARCHAR(64) PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                task_name VARCHAR(255),
                total_processed BIGINT NOT NULL DEFAULT 0,
                success_count BIGINT NOT NULL DEFAULT 0,
                failure_count BIGINT NOT NULL DEFAULT 0,
                retry_count BIGINT NOT NULL DEFAULT 0,
                revoked_count BIGINT NOT NULL DEFAULT 0,
                avg_execution_time_ms DOUBLE PRECISION,
                queue VARCHAR(255)
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.SnapshotsTableName}_timestamp
                ON {_options.Schema}.{_options.SnapshotsTableName} (timestamp);

            CREATE INDEX IF NOT EXISTS idx_{_options.SnapshotsTableName}_task_name
                ON {_options.Schema}.{_options.SnapshotsTableName} (task_name)
                WHERE task_name IS NOT NULL;
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL historical data store table created/verified");
    }

    private static AggregatedMetrics CreateEmptyMetrics(
        DateTimeOffset from,
        DateTimeOffset until,
        MetricsGranularity granularity
    )
    {
        return new AggregatedMetrics
        {
            From = from,
            To = until,
            Granularity = granularity,
            TotalProcessed = 0,
            SuccessCount = 0,
            FailureCount = 0,
            RetryCount = 0,
            RevokedCount = 0,
            TasksPerSecond = 0,
        };
    }

    private static TimeSpan GetBucketSize(MetricsGranularity granularity)
    {
        return granularity switch
        {
            MetricsGranularity.Minute => TimeSpan.FromMinutes(1),
            MetricsGranularity.Hour => TimeSpan.FromHours(1),
            MetricsGranularity.Day => TimeSpan.FromDays(1),
            MetricsGranularity.Week => TimeSpan.FromDays(7),
            _ => TimeSpan.FromHours(1),
        };
    }

    private static string GetTruncateExpression(MetricsGranularity granularity)
    {
        return granularity switch
        {
            MetricsGranularity.Minute => "DATE_TRUNC('minute', timestamp)",
            MetricsGranularity.Hour => "DATE_TRUNC('hour', timestamp)",
            MetricsGranularity.Day => "DATE_TRUNC('day', timestamp)",
            MetricsGranularity.Week => "DATE_TRUNC('week', timestamp)",
            _ => "DATE_TRUNC('hour', timestamp)",
        };
    }
}
