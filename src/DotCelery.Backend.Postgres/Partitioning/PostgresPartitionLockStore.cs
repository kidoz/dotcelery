using DotCelery.Core.Partitioning;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Partitioning;

/// <summary>
/// PostgreSQL implementation of <see cref="IPartitionLockStore"/>.
/// Uses row-level locking for distributed partition locks.
/// </summary>
public sealed class PostgresPartitionLockStore : IPartitionLockStore
{
    private readonly PostgresPartitionLockStoreOptions _options;
    private readonly ILogger<PostgresPartitionLockStore> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresPartitionLockStore"/> class.
    /// </summary>
    public PostgresPartitionLockStore(
        IOptions<PostgresPartitionLockStoreOptions> options,
        ILogger<PostgresPartitionLockStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string partitionKey,
        string taskId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var expiresAt = now.Add(timeout);

        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        try
        {
            // Try to insert or update if expired
            var sql = $"""
                INSERT INTO {_options.Schema}.{_options.TableName}
                    (partition_key, task_id, acquired_at, expires_at)
                VALUES
                    (@partitionKey, @taskId, @acquiredAt, @expiresAt)
                ON CONFLICT (partition_key) DO UPDATE
                SET task_id = @taskId, acquired_at = @acquiredAt, expires_at = @expiresAt
                WHERE {_options.Schema}.{_options.TableName}.expires_at < @now
                   OR {_options.Schema}.{_options.TableName}.task_id = @taskId
                RETURNING partition_key
                """;

            await using var cmd = connection.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = sql;
            cmd.Parameters.AddWithValue("partitionKey", partitionKey);
            cmd.Parameters.AddWithValue("taskId", taskId);
            cmd.Parameters.AddWithValue("acquiredAt", now.UtcDateTime);
            cmd.Parameters.AddWithValue("expiresAt", expiresAt.UtcDateTime);
            cmd.Parameters.AddWithValue("now", now.UtcDateTime);

            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

            if (result is not null)
            {
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogDebug(
                    "Acquired partition lock for {PartitionKey} by task {TaskId}",
                    partitionKey,
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
    public async ValueTask<bool> ReleaseAsync(
        string partitionKey,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE partition_key = @partitionKey AND task_id = @taskId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("partitionKey", partitionKey);
        cmd.Parameters.AddWithValue("taskId", taskId);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogDebug(
                "Released partition lock for {PartitionKey} by task {TaskId}",
                partitionKey,
                taskId
            );
        }

        return deleted > 0;
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsLockedAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT EXISTS(
                SELECT 1 FROM {_options.Schema}.{_options.TableName}
                WHERE partition_key = @partitionKey AND expires_at > NOW()
            )
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("partitionKey", partitionKey);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is bool b && b;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetLockHolderAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT task_id FROM {_options.Schema}.{_options.TableName}
            WHERE partition_key = @partitionKey AND expires_at > NOW()
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("partitionKey", partitionKey);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result as string;
    }

    /// <inheritdoc />
    public async ValueTask<bool> ExtendAsync(
        string partitionKey,
        string taskId,
        TimeSpan extension,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(partitionKey);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var newExpiresAt = DateTimeOffset.UtcNow.Add(extension);

        var sql = $"""
            UPDATE {_options.Schema}.{_options.TableName}
            SET expires_at = @expiresAt
            WHERE partition_key = @partitionKey AND task_id = @taskId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("partitionKey", partitionKey);
        cmd.Parameters.AddWithValue("taskId", taskId);
        cmd.Parameters.AddWithValue("expiresAt", newExpiresAt.UtcDateTime);

        var updated = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return updated > 0;
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
        _logger.LogInformation("PostgreSQL partition lock store disposed");
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
                partition_key VARCHAR(255) PRIMARY KEY,
                task_id VARCHAR(255) NOT NULL,
                acquired_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_expires_at
                ON {_options.Schema}.{_options.TableName} (expires_at);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL partition lock store table created/verified");
    }
}
