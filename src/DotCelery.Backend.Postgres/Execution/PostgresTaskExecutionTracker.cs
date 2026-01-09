using DotCelery.Core.Execution;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Execution;

/// <summary>
/// PostgreSQL implementation of <see cref="ITaskExecutionTracker"/>.
/// </summary>
public sealed class PostgresTaskExecutionTracker : ITaskExecutionTracker
{
    private readonly PostgresTaskExecutionTrackerOptions _options;
    private readonly ILogger<PostgresTaskExecutionTracker> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresTaskExecutionTracker"/> class.
    /// </summary>
    public PostgresTaskExecutionTracker(
        IOptions<PostgresTaskExecutionTrackerOptions> options,
        ILogger<PostgresTaskExecutionTracker> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryStartAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var lockKey = GetLockKey(taskName, key);
        var now = DateTimeOffset.UtcNow;
        var expiresAt = now.Add(timeout ?? _options.DefaultTimeout);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            // Try to insert or update if expired or same task
            var sql = $"""
                INSERT INTO {_options.Schema}.{_options.TableName}
                    (lock_key, task_id, execution_key, started_at, expires_at)
                VALUES
                    (@lockKey, @taskId, @executionKey, @startedAt, @expiresAt)
                ON CONFLICT (lock_key) DO UPDATE
                SET task_id = @taskId, started_at = @startedAt, expires_at = @expiresAt
                WHERE {_options.Schema}.{_options.TableName}.expires_at < @now
                   OR {_options.Schema}.{_options.TableName}.task_id = @taskId
                RETURNING lock_key
                """;

            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("lockKey", lockKey);
            cmd.Parameters.AddWithValue("taskId", taskId);
            cmd.Parameters.AddWithValue("executionKey", key ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("startedAt", now.UtcDateTime);
            cmd.Parameters.AddWithValue("expiresAt", expiresAt.UtcDateTime);
            cmd.Parameters.AddWithValue("now", now.UtcDateTime);

            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

            if (result is not null)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogDebug(
                    "Started execution tracking for {TaskName} task {TaskId}",
                    taskName,
                    taskId
                );
                return true;
            }

            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }
        catch (PostgresException ex) when (ex.SqlState == "23505") // Unique violation
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            return false;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask StopAsync(
        string taskName,
        string taskId,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var lockKey = GetLockKey(taskName, key);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE lock_key = @lockKey AND task_id = @taskId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("lockKey", lockKey);
        cmd.Parameters.AddWithValue("taskId", taskId);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug(
            "Stopped execution tracking for {TaskName} task {TaskId}",
            taskName,
            taskId
        );
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsExecutingAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var lockKey = GetLockKey(taskName, key);

        var sql = $"""
            SELECT EXISTS(
                SELECT 1 FROM {_options.Schema}.{_options.TableName}
                WHERE lock_key = @lockKey AND expires_at > NOW()
            )
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("lockKey", lockKey);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is bool b && b;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetExecutingTaskIdAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var lockKey = GetLockKey(taskName, key);

        var sql = $"""
            SELECT task_id FROM {_options.Schema}.{_options.TableName}
            WHERE lock_key = @lockKey AND expires_at > NOW()
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("lockKey", lockKey);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result as string;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? extension = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskName);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var lockKey = GetLockKey(taskName, key);
        var newExpiresAt = DateTimeOffset.UtcNow.Add(extension ?? _options.DefaultTimeout);

        var sql = $"""
            UPDATE {_options.Schema}.{_options.TableName}
            SET expires_at = @expiresAt
            WHERE lock_key = @lockKey AND task_id = @taskId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("lockKey", lockKey);
        cmd.Parameters.AddWithValue("taskId", taskId);
        cmd.Parameters.AddWithValue("expiresAt", newExpiresAt.UtcDateTime);

        var updated = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return updated > 0;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, ExecutingTaskInfo>> GetAllExecutingAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT lock_key, task_id, execution_key, started_at, expires_at
            FROM {_options.Schema}.{_options.TableName}
            WHERE expires_at > NOW()
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        var result = new Dictionary<string, ExecutingTaskInfo>();

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var lockKey = reader.GetString(0);
            result[lockKey] = new ExecutingTaskInfo
            {
                TaskId = reader.GetString(1),
                Key = reader.IsDBNull(2) ? null : reader.GetString(2),
                StartedAt = new DateTimeOffset(reader.GetDateTime(3), TimeSpan.Zero),
                ExpiresAt = new DateTimeOffset(reader.GetDateTime(4), TimeSpan.Zero),
            };
        }

        return result;
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
        _logger.LogInformation("PostgreSQL task execution tracker disposed");
    }

    private static string GetLockKey(string taskName, string? key)
    {
        return key is null ? taskName : $"{taskName}:{key}";
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
                lock_key VARCHAR(511) PRIMARY KEY,
                task_id VARCHAR(255) NOT NULL,
                execution_key VARCHAR(255),
                started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_expires_at
                ON {_options.Schema}.{_options.TableName} (expires_at);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL task execution tracker table created/verified");
    }
}
