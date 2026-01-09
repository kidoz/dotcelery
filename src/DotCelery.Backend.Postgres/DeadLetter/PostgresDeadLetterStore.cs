using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.DeadLetter;

/// <summary>
/// PostgreSQL implementation of <see cref="IDeadLetterStore"/>.
/// </summary>
public sealed class PostgresDeadLetterStore : IDeadLetterStore
{
    private readonly PostgresDeadLetterStoreOptions _options;
    private readonly ILogger<PostgresDeadLetterStore> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresDeadLetterStore"/> class.
    /// </summary>
    public PostgresDeadLetterStore(
        IOptions<PostgresDeadLetterStoreOptions> options,
        ILogger<PostgresDeadLetterStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        DeadLetterMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var expiresAt = message.ExpiresAt ?? DateTimeOffset.UtcNow.Add(_options.DefaultRetention);

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.TableName}
                (id, task_id, task_name, queue, reason, original_message, exception_message,
                 exception_type, stack_trace, retry_count, timestamp, expires_at, worker)
            VALUES
                (@id, @taskId, @taskName, @queue, @reason, @originalMessage, @exceptionMessage,
                 @exceptionType, @stackTrace, @retryCount, @timestamp, @expiresAt, @worker)
            ON CONFLICT (id) DO UPDATE SET
                task_id = @taskId,
                task_name = @taskName,
                queue = @queue,
                reason = @reason,
                original_message = @originalMessage,
                exception_message = @exceptionMessage,
                exception_type = @exceptionType,
                stack_trace = @stackTrace,
                retry_count = @retryCount,
                timestamp = @timestamp,
                expires_at = @expiresAt,
                worker = @worker
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", message.Id);
        cmd.Parameters.AddWithValue("taskId", message.TaskId);
        cmd.Parameters.AddWithValue("taskName", message.TaskName);
        cmd.Parameters.AddWithValue("queue", message.Queue);
        cmd.Parameters.AddWithValue("reason", message.Reason.ToString());
        cmd.Parameters.AddWithValue("originalMessage", message.OriginalMessage);
        cmd.Parameters.AddWithValue(
            "exceptionMessage",
            message.ExceptionMessage ?? (object)DBNull.Value
        );
        cmd.Parameters.AddWithValue("exceptionType", message.ExceptionType ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("stackTrace", message.StackTrace ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("retryCount", message.RetryCount);
        cmd.Parameters.AddWithValue("timestamp", message.Timestamp.UtcDateTime);
        cmd.Parameters.AddWithValue("expiresAt", expiresAt.UtcDateTime);
        cmd.Parameters.AddWithValue("worker", message.Worker ?? (object)DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug(
            "Stored dead letter message {MessageId} for task {TaskId}",
            message.Id,
            message.TaskId
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<DeadLetterMessage> GetAllAsync(
        int limit = 100,
        int offset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT id, task_id, task_name, queue, reason, original_message, exception_message,
                   exception_type, stack_trace, retry_count, timestamp, expires_at, worker
            FROM {_options.Schema}.{_options.TableName}
            WHERE expires_at > NOW()
            ORDER BY timestamp DESC
            LIMIT @limit OFFSET @offset
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("limit", limit);
        cmd.Parameters.AddWithValue("offset", offset);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return ReadDeadLetterMessage(reader);
        }
    }

    /// <inheritdoc />
    public async ValueTask<DeadLetterMessage?> GetAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT id, task_id, task_name, queue, reason, original_message, exception_message,
                   exception_type, stack_trace, retry_count, timestamp, expires_at, worker
            FROM {_options.Schema}.{_options.TableName}
            WHERE id = @id AND expires_at > NOW()
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", messageId);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return ReadDeadLetterMessage(reader);
    }

    /// <inheritdoc />
    public async ValueTask<bool> RequeueAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // For PostgreSQL, requeue means delete and let the caller handle republishing
        return await DeleteAsync(messageId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE id = @id
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", messageId);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogDebug("Deleted dead letter message {MessageId}", messageId);
        }

        return deleted > 0;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COUNT(*) FROM {_options.Schema}.{_options.TableName}
            WHERE expires_at > NOW()
            """;

        await using var cmd = _dataSource.CreateCommand(sql);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async ValueTask<long> PurgeAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            """;

        await using var cmd = _dataSource.CreateCommand(sql);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Purged {Count} dead letter messages", deleted);
        return deleted;
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupExpiredAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE expires_at <= NOW()
            """;

        await using var cmd = _dataSource.CreateCommand(sql);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogInformation("Cleaned up {Count} expired dead letter messages", deleted);
        }

        return deleted;
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
        _logger.LogInformation("PostgreSQL dead letter store disposed");
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
                id VARCHAR(255) PRIMARY KEY,
                task_id VARCHAR(255) NOT NULL,
                task_name VARCHAR(255) NOT NULL,
                queue VARCHAR(255) NOT NULL,
                reason VARCHAR(50) NOT NULL,
                original_message BYTEA NOT NULL,
                exception_message TEXT,
                exception_type VARCHAR(500),
                stack_trace TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                worker VARCHAR(255)
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_expires_at
                ON {_options.Schema}.{_options.TableName} (expires_at);

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_timestamp
                ON {_options.Schema}.{_options.TableName} (timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_task_id
                ON {_options.Schema}.{_options.TableName} (task_id);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL dead letter store table created/verified");
    }

    private static DeadLetterMessage ReadDeadLetterMessage(NpgsqlDataReader reader)
    {
        var reasonStr = reader.GetString(4);
        var reason = Enum.TryParse<DeadLetterReason>(reasonStr, out var r)
            ? r
            : DeadLetterReason.Failed;

        return new DeadLetterMessage
        {
            Id = reader.GetString(0),
            TaskId = reader.GetString(1),
            TaskName = reader.GetString(2),
            Queue = reader.GetString(3),
            Reason = reason,
            OriginalMessage = (byte[])reader[5],
            ExceptionMessage = reader.IsDBNull(6) ? null : reader.GetString(6),
            ExceptionType = reader.IsDBNull(7) ? null : reader.GetString(7),
            StackTrace = reader.IsDBNull(8) ? null : reader.GetString(8),
            RetryCount = reader.GetInt32(9),
            Timestamp = new DateTimeOffset(reader.GetDateTime(10), TimeSpan.Zero),
            ExpiresAt = reader.IsDBNull(11)
                ? null
                : new DateTimeOffset(reader.GetDateTime(11), TimeSpan.Zero),
            Worker = reader.IsDBNull(12) ? null : reader.GetString(12),
        };
    }
}
