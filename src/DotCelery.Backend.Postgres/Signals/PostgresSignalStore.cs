using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Signals;

/// <summary>
/// PostgreSQL implementation of <see cref="ISignalStore"/>.
/// Provides durable signal queue with visibility timeout pattern using SELECT FOR UPDATE SKIP LOCKED.
/// </summary>
public sealed class PostgresSignalStore : ISignalStore
{
    private readonly PostgresSignalStoreOptions _options;
    private readonly ILogger<PostgresSignalStore> _logger;
    private readonly NpgsqlDataSource _dataSource;
    private readonly JsonSerializerOptions _jsonOptions;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresSignalStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    public PostgresSignalStore(
        IOptions<PostgresSignalStoreOptions> options,
        ILogger<PostgresSignalStore> logger
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
    public async ValueTask EnqueueAsync(
        SignalMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.TableName}
                (id, signal_type, task_id, task_name, payload, created_at, status, visible_at)
            VALUES
                (@id, @signalType, @taskId, @taskName, @payload, @createdAt, 0, @visibleAt)
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", message.Id);
        cmd.Parameters.AddWithValue("signalType", message.SignalType);
        cmd.Parameters.AddWithValue("taskId", message.TaskId);
        cmd.Parameters.AddWithValue("taskName", message.TaskName);
        cmd.Parameters.AddWithValue("payload", message.Payload);
        cmd.Parameters.AddWithValue("createdAt", message.CreatedAt.UtcDateTime);
        cmd.Parameters.AddWithValue("visibleAt", DateTime.UtcNow);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug(
            "Enqueued signal {SignalId} of type {SignalType} for task {TaskId}",
            message.Id,
            message.SignalType,
            message.TaskId
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SignalMessage> DequeueAsync(
        int batchSize = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var visibilityTimeout = now.Add(_options.VisibilityTimeout);

        // Use a transaction to atomically claim messages
        await using var connection = await _dataSource
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(cancellationToken)
            .ConfigureAwait(false);

        // Select and lock pending messages that are visible
        var selectSql = $"""
            SELECT id, signal_type, task_id, task_name, payload, created_at
            FROM {_options.Schema}.{_options.TableName}
            WHERE status = 0 AND visible_at <= @now
            ORDER BY created_at
            LIMIT @limit
            FOR UPDATE SKIP LOCKED
            """;

        await using var selectCmd = new NpgsqlCommand(selectSql, connection, transaction);
        selectCmd.Parameters.AddWithValue("now", now);
        selectCmd.Parameters.AddWithValue("limit", batchSize);

        var messages = new List<SignalMessage>();

        await using (
            var reader = await selectCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                messages.Add(
                    new SignalMessage
                    {
                        Id = reader.GetString(0),
                        SignalType = reader.GetString(1),
                        TaskId = reader.GetString(2),
                        TaskName = reader.GetString(3),
                        Payload = reader.GetString(4),
                        CreatedAt = new DateTimeOffset(reader.GetDateTime(5), TimeSpan.Zero),
                    }
                );
            }
        }

        if (messages.Count > 0)
        {
            // Mark selected messages as processing
            var ids = messages.Select(m => m.Id).ToArray();
            var updateSql = $"""
                UPDATE {_options.Schema}.{_options.TableName}
                SET status = 1, visible_at = @visibleAt
                WHERE id = ANY(@ids)
                """;

            await using var updateCmd = new NpgsqlCommand(updateSql, connection, transaction);
            updateCmd.Parameters.AddWithValue("visibleAt", visibilityTimeout);
            updateCmd.Parameters.AddWithValue("ids", ids);

            await updateCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

        foreach (var message in messages)
        {
            yield return message;
        }
    }

    /// <inheritdoc />
    public async ValueTask AcknowledgeAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE id = @id AND status = 1
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("id", messageId);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogDebug("Acknowledged signal {SignalId}", messageId);
        }
    }

    /// <inheritdoc />
    public async ValueTask RejectAsync(
        string messageId,
        bool requeue = true,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        if (requeue)
        {
            // Put back in queue with immediate visibility
            var sql = $"""
                UPDATE {_options.Schema}.{_options.TableName}
                SET status = 0, visible_at = @visibleAt
                WHERE id = @id
                """;

            await using var cmd = _dataSource.CreateCommand(sql);
            cmd.Parameters.AddWithValue("id", messageId);
            cmd.Parameters.AddWithValue("visibleAt", DateTime.UtcNow);

            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Requeued signal {SignalId}", messageId);
        }
        else
        {
            // Delete permanently
            var sql = $"""
                DELETE FROM {_options.Schema}.{_options.TableName}
                WHERE id = @id
                """;

            await using var cmd = _dataSource.CreateCommand(sql);
            cmd.Parameters.AddWithValue("id", messageId);

            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            _logger.LogDebug("Rejected and deleted signal {SignalId}", messageId);
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COUNT(*) FROM {_options.Schema}.{_options.TableName}
            WHERE status = 0
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

        _logger.LogInformation("PostgreSQL signal store disposed");
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
                signal_type TEXT NOT NULL,
                task_id VARCHAR(255) NOT NULL,
                task_name VARCHAR(512) NOT NULL,
                payload TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                status INTEGER NOT NULL DEFAULT 0,
                visible_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_status_visible
                ON {_options.Schema}.{_options.TableName} (status, visible_at, created_at)
                WHERE status = 0;

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_task_id
                ON {_options.Schema}.{_options.TableName} (task_id);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL signal store table created/verified");
    }
}
