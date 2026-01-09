using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Metrics;

/// <summary>
/// PostgreSQL implementation of <see cref="IQueueMetrics"/>.
/// </summary>
public sealed class PostgresQueueMetrics : IQueueMetrics, IAsyncDisposable
{
    private readonly PostgresQueueMetricsOptions _options;
    private readonly ILogger<PostgresQueueMetrics> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresQueueMetrics"/> class.
    /// </summary>
    public PostgresQueueMetrics(
        IOptions<PostgresQueueMetricsOptions> options,
        ILogger<PostgresQueueMetrics> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetWaitingCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COALESCE(waiting_count, 0) FROM {_options.Schema}.{_options.MetricsTableName}
            WHERE queue = @queue
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("queue", queue);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull
            ? 0
            : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetRunningCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COALESCE(running_count, 0) FROM {_options.Schema}.{_options.MetricsTableName}
            WHERE queue = @queue
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("queue", queue);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull
            ? 0
            : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetProcessedCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COALESCE(processed_count, 0) FROM {_options.Schema}.{_options.MetricsTableName}
            WHERE queue = @queue
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("queue", queue);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull
            ? 0
            : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async ValueTask<int> GetConsumerCountAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COALESCE(consumer_count, 0) FROM {_options.Schema}.{_options.MetricsTableName}
            WHERE queue = @queue
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("queue", queue);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull
            ? 0
            : Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> GetQueuesAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT queue FROM {_options.Schema}.{_options.MetricsTableName}
            ORDER BY queue
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        var queues = new List<string>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            queues.Add(reader.GetString(0));
        }

        return queues;
    }

    /// <inheritdoc />
    public async ValueTask<QueueMetricsData> GetMetricsAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT queue, waiting_count, running_count, processed_count, success_count,
                   failure_count, consumer_count, total_duration_ms, completed_count,
                   last_enqueued_at, last_completed_at
            FROM {_options.Schema}.{_options.MetricsTableName}
            WHERE queue = @queue
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("queue", queue);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return new QueueMetricsData { Queue = queue };
        }

        return ReadQueueMetrics(reader);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, QueueMetricsData>> GetAllMetricsAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT queue, waiting_count, running_count, processed_count, success_count,
                   failure_count, consumer_count, total_duration_ms, completed_count,
                   last_enqueued_at, last_completed_at
            FROM {_options.Schema}.{_options.MetricsTableName}
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        var result = new Dictionary<string, QueueMetricsData>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var metrics = ReadQueueMetrics(reader);
            result[metrics.Queue] = metrics;
        }

        return result;
    }

    /// <inheritdoc />
    public async ValueTask RecordStartedAsync(
        string queue,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            // Ensure queue exists and update metrics
            var metricsSql = $"""
                INSERT INTO {_options.Schema}.{_options.MetricsTableName}
                    (queue, running_count, waiting_count)
                VALUES
                    (@queue, 1, -1)
                ON CONFLICT (queue) DO UPDATE
                SET running_count = {_options.Schema}.{_options.MetricsTableName}.running_count + 1,
                    waiting_count = GREATEST(0, {_options.Schema}.{_options.MetricsTableName}.waiting_count - 1)
                """;

            await using var metricsCmd = connection.CreateCommand();
            metricsCmd.Transaction = transaction;
            metricsCmd.CommandText = metricsSql;
            metricsCmd.Parameters.AddWithValue("queue", queue);
            await metricsCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Track running task
            var taskSql = $"""
                INSERT INTO {_options.Schema}.{_options.RunningTasksTableName}
                    (task_id, queue, started_at)
                VALUES
                    (@taskId, @queue, @startedAt)
                ON CONFLICT (task_id) DO NOTHING
                """;

            await using var taskCmd = connection.CreateCommand();
            taskCmd.Transaction = transaction;
            taskCmd.CommandText = taskSql;
            taskCmd.Parameters.AddWithValue("taskId", taskId);
            taskCmd.Parameters.AddWithValue("queue", queue);
            taskCmd.Parameters.AddWithValue("startedAt", DateTimeOffset.UtcNow.UtcDateTime);
            await taskCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask RecordCompletedAsync(
        string queue,
        string taskId,
        bool success,
        TimeSpan duration,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            var successIncrement = success ? 1 : 0;
            var failureIncrement = success ? 0 : 1;

            var sql = $"""
                UPDATE {_options.Schema}.{_options.MetricsTableName}
                SET running_count = GREATEST(0, running_count - 1),
                    processed_count = processed_count + 1,
                    success_count = success_count + @successIncrement,
                    failure_count = failure_count + @failureIncrement,
                    total_duration_ms = total_duration_ms + @durationMs,
                    completed_count = completed_count + 1,
                    last_completed_at = @completedAt
                WHERE queue = @queue
                """;

            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("queue", queue);
            cmd.Parameters.AddWithValue("successIncrement", successIncrement);
            cmd.Parameters.AddWithValue("failureIncrement", failureIncrement);
            cmd.Parameters.AddWithValue("durationMs", (long)duration.TotalMilliseconds);
            cmd.Parameters.AddWithValue("completedAt", DateTimeOffset.UtcNow.UtcDateTime);

            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Remove from running tasks
            var taskSql = $"""
                DELETE FROM {_options.Schema}.{_options.RunningTasksTableName}
                WHERE task_id = @taskId
                """;

            await using var taskCmd = connection.CreateCommand();
            taskCmd.Transaction = transaction;
            taskCmd.CommandText = taskSql;
            taskCmd.Parameters.AddWithValue("taskId", taskId);
            await taskCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask RecordEnqueuedAsync(
        string queue,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.MetricsTableName}
                (queue, waiting_count, last_enqueued_at)
            VALUES
                (@queue, 1, @enqueuedAt)
            ON CONFLICT (queue) DO UPDATE
            SET waiting_count = {_options.Schema}.{_options.MetricsTableName}.waiting_count + 1,
                last_enqueued_at = @enqueuedAt
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("queue", queue);
        cmd.Parameters.AddWithValue("enqueuedAt", DateTimeOffset.UtcNow.UtcDateTime);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
        _logger.LogInformation("PostgreSQL queue metrics disposed");
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
            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.MetricsTableName} (
                queue VARCHAR(255) PRIMARY KEY,
                waiting_count BIGINT NOT NULL DEFAULT 0,
                running_count BIGINT NOT NULL DEFAULT 0,
                processed_count BIGINT NOT NULL DEFAULT 0,
                success_count BIGINT NOT NULL DEFAULT 0,
                failure_count BIGINT NOT NULL DEFAULT 0,
                consumer_count INTEGER NOT NULL DEFAULT 0,
                total_duration_ms BIGINT NOT NULL DEFAULT 0,
                completed_count BIGINT NOT NULL DEFAULT 0,
                last_enqueued_at TIMESTAMP WITH TIME ZONE,
                last_completed_at TIMESTAMP WITH TIME ZONE
            );

            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.RunningTasksTableName} (
                task_id VARCHAR(255) PRIMARY KEY,
                queue VARCHAR(255) NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.RunningTasksTableName}_queue
                ON {_options.Schema}.{_options.RunningTasksTableName} (queue);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL queue metrics tables created/verified");
    }

    private static QueueMetricsData ReadQueueMetrics(NpgsqlDataReader reader)
    {
        var totalDurationMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7);
        var completedCount = reader.IsDBNull(8) ? 0 : reader.GetInt64(8);
        var averageDuration =
            completedCount > 0
                ? TimeSpan.FromMilliseconds(totalDurationMs / completedCount)
                : (TimeSpan?)null;

        return new QueueMetricsData
        {
            Queue = reader.GetString(0),
            WaitingCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
            RunningCount = reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
            ProcessedCount = reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
            SuccessCount = reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
            FailureCount = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
            ConsumerCount = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
            AverageDuration = averageDuration,
            LastEnqueuedAt = reader.IsDBNull(9)
                ? null
                : new DateTimeOffset(reader.GetDateTime(9), TimeSpan.Zero),
            LastCompletedAt = reader.IsDBNull(10)
                ? null
                : new DateTimeOffset(reader.GetDateTime(10), TimeSpan.Zero),
        };
    }
}
