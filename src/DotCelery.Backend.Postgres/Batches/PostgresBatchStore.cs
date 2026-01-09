using System.Text.Json;
using DotCelery.Core.Batches;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Batches;

/// <summary>
/// PostgreSQL implementation of <see cref="IBatchStore"/>.
/// </summary>
public sealed class PostgresBatchStore : IBatchStore
{
    private readonly PostgresBatchStoreOptions _options;
    private readonly ILogger<PostgresBatchStore> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresBatchStore"/> class.
    /// </summary>
    public PostgresBatchStore(
        IOptions<PostgresBatchStoreOptions> options,
        ILogger<PostgresBatchStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Batch batch, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(batch);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            // Insert batch
            var batchSql = $"""
                INSERT INTO {_options.Schema}.{_options.BatchesTableName}
                    (id, name, state, callback_task_id, created_at, completed_at)
                VALUES
                    (@id, @name, @state, @callbackTaskId, @createdAt, @completedAt)
                """;

            await using var batchCmd = connection.CreateCommand();
            batchCmd.Transaction = transaction;
            batchCmd.CommandText = batchSql;
            batchCmd.Parameters.AddWithValue("id", batch.Id);
            batchCmd.Parameters.AddWithValue("name", batch.Name ?? (object)DBNull.Value);
            batchCmd.Parameters.AddWithValue("state", batch.State.ToString());
            batchCmd.Parameters.AddWithValue(
                "callbackTaskId",
                batch.CallbackTaskId ?? (object)DBNull.Value
            );
            batchCmd.Parameters.AddWithValue("createdAt", batch.CreatedAt.UtcDateTime);
            batchCmd.Parameters.AddWithValue(
                "completedAt",
                batch.CompletedAt?.UtcDateTime ?? (object)DBNull.Value
            );

            await batchCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Insert task mappings
            foreach (var taskId in batch.TaskIds)
            {
                var isCompleted = batch.CompletedTaskIds.Contains(taskId);
                var isFailed = batch.FailedTaskIds.Contains(taskId);

                var taskSql = $"""
                    INSERT INTO {_options.Schema}.{_options.BatchTasksTableName}
                        (batch_id, task_id, is_completed, is_failed)
                    VALUES
                        (@batchId, @taskId, @isCompleted, @isFailed)
                    """;

                await using var taskCmd = connection.CreateCommand();
                taskCmd.Transaction = transaction;
                taskCmd.CommandText = taskSql;
                taskCmd.Parameters.AddWithValue("batchId", batch.Id);
                taskCmd.Parameters.AddWithValue("taskId", taskId);
                taskCmd.Parameters.AddWithValue("isCompleted", isCompleted);
                taskCmd.Parameters.AddWithValue("isFailed", isFailed);
                await taskCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }

        _logger.LogDebug("Created batch {BatchId} with {Count} tasks", batch.Id, batch.TotalTasks);
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> GetAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT b.id, b.name, b.state, b.callback_task_id, b.created_at, b.completed_at,
                   ARRAY_AGG(t.task_id) FILTER (WHERE t.task_id IS NOT NULL) as task_ids,
                   ARRAY_AGG(t.task_id) FILTER (WHERE t.is_completed) as completed_task_ids,
                   ARRAY_AGG(t.task_id) FILTER (WHERE t.is_failed) as failed_task_ids
            FROM {_options.Schema}.{_options.BatchesTableName} b
            LEFT JOIN {_options.Schema}.{_options.BatchTasksTableName} t ON b.id = t.batch_id
            WHERE b.id = @batchId
            GROUP BY b.id
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("batchId", batchId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return ReadBatch(reader);
    }

    /// <inheritdoc />
    public async ValueTask UpdateStateAsync(
        string batchId,
        BatchState state,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            UPDATE {_options.Schema}.{_options.BatchesTableName}
            SET state = @state,
                completed_at = CASE WHEN @state IN ('Completed', 'Failed') THEN NOW() ELSE completed_at END
            WHERE id = @batchId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("batchId", batchId);
        cmd.Parameters.AddWithValue("state", state.ToString());

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> MarkTaskCompletedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            UPDATE {_options.Schema}.{_options.BatchTasksTableName}
            SET is_completed = TRUE
            WHERE batch_id = @batchId AND task_id = @taskId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("batchId", batchId);
        cmd.Parameters.AddWithValue("taskId", taskId);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        return await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> MarkTaskFailedAsync(
        string batchId,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            UPDATE {_options.Schema}.{_options.BatchTasksTableName}
            SET is_failed = TRUE
            WHERE batch_id = @batchId AND task_id = @taskId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("batchId", batchId);
        cmd.Parameters.AddWithValue("taskId", taskId);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        return await GetAsync(batchId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            // Delete task mappings first
            var taskSql = $"""
                DELETE FROM {_options.Schema}.{_options.BatchTasksTableName}
                WHERE batch_id = @batchId
                """;

            await using var taskCmd = connection.CreateCommand();
            taskCmd.Transaction = transaction;
            taskCmd.CommandText = taskSql;
            taskCmd.Parameters.AddWithValue("batchId", batchId);
            await taskCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            // Delete batch
            var batchSql = $"""
                DELETE FROM {_options.Schema}.{_options.BatchesTableName}
                WHERE id = @batchId
                """;

            await using var batchCmd = connection.CreateCommand();
            batchCmd.Transaction = transaction;
            batchCmd.CommandText = batchSql;
            batchCmd.Parameters.AddWithValue("batchId", batchId);
            var deleted = await batchCmd
                .ExecuteNonQueryAsync(cancellationToken)
                .ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            return deleted > 0;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetBatchIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT batch_id FROM {_options.Schema}.{_options.BatchTasksTableName}
            WHERE task_id = @taskId
            LIMIT 1
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("taskId", taskId);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result as string;
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
        _logger.LogInformation("PostgreSQL batch store disposed");
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
            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.BatchesTableName} (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                state VARCHAR(50) NOT NULL,
                callback_task_id VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMP WITH TIME ZONE
            );

            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.BatchTasksTableName} (
                batch_id VARCHAR(255) NOT NULL,
                task_id VARCHAR(255) NOT NULL,
                is_completed BOOLEAN NOT NULL DEFAULT FALSE,
                is_failed BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (batch_id, task_id),
                FOREIGN KEY (batch_id) REFERENCES {_options.Schema}.{_options.BatchesTableName}(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.BatchTasksTableName}_task_id
                ON {_options.Schema}.{_options.BatchTasksTableName} (task_id);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL batch store tables created/verified");
    }

    private static Batch ReadBatch(NpgsqlDataReader reader)
    {
        var stateStr = reader.GetString(2);
        var state = Enum.TryParse<BatchState>(stateStr, out var s) ? s : BatchState.Pending;

        var taskIds = reader.IsDBNull(6) ? [] : (string[])reader[6];
        var completedTaskIds = reader.IsDBNull(7) ? [] : (string[])reader[7];
        var failedTaskIds = reader.IsDBNull(8) ? [] : (string[])reader[8];

        return new Batch
        {
            Id = reader.GetString(0),
            Name = reader.IsDBNull(1) ? null : reader.GetString(1),
            State = state,
            CallbackTaskId = reader.IsDBNull(3) ? null : reader.GetString(3),
            CreatedAt = new DateTimeOffset(reader.GetDateTime(4), TimeSpan.Zero),
            CompletedAt = reader.IsDBNull(5)
                ? null
                : new DateTimeOffset(reader.GetDateTime(5), TimeSpan.Zero),
            TaskIds = taskIds.ToList(),
            CompletedTaskIds = completedTaskIds.ToList(),
            FailedTaskIds = failedTaskIds.ToList(),
        };
    }
}
