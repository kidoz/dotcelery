using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Outbox;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Outbox;

/// <summary>
/// PostgreSQL implementation of <see cref="IOutboxStore"/>.
/// Provides durable, transactional outbox pattern support.
/// </summary>
public sealed class PostgresOutboxStore : IOutboxStore
{
    private readonly PostgresOutboxStoreOptions _options;
    private readonly ILogger<PostgresOutboxStore> _logger;
    private readonly NpgsqlDataSource _dataSource;

    // AOT-friendly type info
    private static JsonTypeInfo<TaskMessage> TaskMessageTypeInfo =>
        DotCeleryJsonContext.Default.TaskMessage;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresOutboxStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    public PostgresOutboxStore(
        IOptions<PostgresOutboxStoreOptions> options,
        ILogger<PostgresOutboxStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask StoreAsync(
        OutboxMessage message,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var taskMessageJson = JsonSerializer.Serialize(message.TaskMessage, TaskMessageTypeInfo);

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.TableName}
                (id, task_message, status, created_at, attempts, sequence_number)
            VALUES
                (@id, @taskMessage::jsonb, @status, @createdAt, @attempts, nextval('{_options.Schema}.{_options.TableName}_seq'))
            ON CONFLICT (id) DO UPDATE SET
                task_message = @taskMessage::jsonb,
                status = @status,
                attempts = @attempts
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", message.Id);
        cmd.Parameters.AddWithValue("taskMessage", taskMessageJson);
        cmd.Parameters.AddWithValue("status", (int)message.Status);
        cmd.Parameters.AddWithValue("createdAt", message.CreatedAt.UtcDateTime);
        cmd.Parameters.AddWithValue("attempts", message.Attempts);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Stored outbox message {MessageId}", message.Id);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<OutboxMessage> GetPendingAsync(
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT id, task_message, status, created_at, attempts, last_error, dispatched_at, sequence_number
            FROM {_options.Schema}.{_options.TableName}
            WHERE status = @status
            ORDER BY sequence_number
            LIMIT @limit
            FOR UPDATE SKIP LOCKED
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("status", (int)OutboxMessageStatus.Pending);
        cmd.Parameters.AddWithValue("limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var taskMessageJson = reader.GetString(1);
            var taskMessage = JsonSerializer.Deserialize(taskMessageJson, TaskMessageTypeInfo)!;

            yield return new OutboxMessage
            {
                Id = reader.GetString(0),
                TaskMessage = taskMessage,
                Status = (OutboxMessageStatus)reader.GetInt32(2),
                CreatedAt = new DateTimeOffset(reader.GetDateTime(3), TimeSpan.Zero),
                Attempts = reader.GetInt32(4),
                LastError = reader.IsDBNull(5) ? null : reader.GetString(5),
                DispatchedAt = reader.IsDBNull(6)
                    ? null
                    : new DateTimeOffset(reader.GetDateTime(6), TimeSpan.Zero),
                SequenceNumber = reader.GetInt64(7),
            };
        }
    }

    /// <inheritdoc />
    public async ValueTask MarkDispatchedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            UPDATE {_options.Schema}.{_options.TableName}
            SET status = @status, dispatched_at = @dispatchedAt
            WHERE id = @id
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", messageId);
        cmd.Parameters.AddWithValue("status", (int)OutboxMessageStatus.Dispatched);
        cmd.Parameters.AddWithValue("dispatchedAt", DateTime.UtcNow);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Marked outbox message {MessageId} as dispatched", messageId);
    }

    /// <inheritdoc />
    public async ValueTask MarkFailedAsync(
        string messageId,
        string errorMessage,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            UPDATE {_options.Schema}.{_options.TableName}
            SET
                attempts = attempts + 1,
                last_error = @lastError,
                status = CASE WHEN attempts + 1 >= 5 THEN @failedStatus ELSE status END
            WHERE id = @id
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", messageId);
        cmd.Parameters.AddWithValue("lastError", errorMessage);
        cmd.Parameters.AddWithValue("failedStatus", (int)OutboxMessageStatus.Failed);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Marked outbox message {MessageId} as failed", messageId);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COUNT(*) FROM {_options.Schema}.{_options.TableName}
            WHERE status = @status
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("status", (int)OutboxMessageStatus.Pending);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var cutoff = DateTime.UtcNow - olderThan;

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE (status = @dispatchedStatus AND dispatched_at < @cutoff)
               OR (status = @failedStatus AND created_at < @cutoff)
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("dispatchedStatus", (int)OutboxMessageStatus.Dispatched);
        cmd.Parameters.AddWithValue("failedStatus", (int)OutboxMessageStatus.Failed);
        cmd.Parameters.AddWithValue("cutoff", cutoff);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Cleaned up {Count} outbox messages", deleted);
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

        _logger.LogInformation("PostgreSQL outbox store disposed");
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
            CREATE SEQUENCE IF NOT EXISTS {_options.Schema}.{_options.TableName}_seq;

            CREATE TABLE IF NOT EXISTS {_options.Schema}.{_options.TableName} (
                id VARCHAR(255) PRIMARY KEY,
                task_message JSONB NOT NULL,
                status INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                dispatched_at TIMESTAMP WITH TIME ZONE,
                sequence_number BIGINT NOT NULL DEFAULT nextval('{_options.Schema}.{_options.TableName}_seq')
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_status_seq
                ON {_options.Schema}.{_options.TableName} (status, sequence_number)
                WHERE status = 0;
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL outbox table created/verified");
    }
}
