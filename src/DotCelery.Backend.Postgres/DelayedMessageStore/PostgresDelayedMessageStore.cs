using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.DelayedMessageStore;

/// <summary>
/// PostgreSQL implementation of <see cref="IDelayedMessageStore"/>.
/// Uses a table with delivery_time column for scheduling.
/// </summary>
public sealed class PostgresDelayedMessageStore : IDelayedMessageStore
{
    private readonly PostgresDelayedMessageStoreOptions _options;
    private readonly ILogger<PostgresDelayedMessageStore> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private static JsonTypeInfo<TaskMessage> TaskMessageTypeInfo =>
        DotCeleryJsonContext.Default.TaskMessage;

    private bool _disposed;
    private bool _initialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresDelayedMessageStore"/> class.
    /// </summary>
    public PostgresDelayedMessageStore(
        IOptions<PostgresDelayedMessageStoreOptions> options,
        ILogger<PostgresDelayedMessageStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = NpgsqlDataSource.Create(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask AddAsync(
        TaskMessage message,
        DateTimeOffset deliveryTime,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var json = JsonSerializer.Serialize(message, TaskMessageTypeInfo);

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.TableName}
                (task_id, message, delivery_time, created_at)
            VALUES
                (@taskId, @message::jsonb, @deliveryTime, @createdAt)
            ON CONFLICT (task_id) DO UPDATE SET
                message = @message::jsonb,
                delivery_time = @deliveryTime
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("taskId", message.Id);
        cmd.Parameters.AddWithValue("message", json);
        cmd.Parameters.AddWithValue("deliveryTime", deliveryTime.UtcDateTime);
        cmd.Parameters.AddWithValue("createdAt", DateTimeOffset.UtcNow.UtcDateTime);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug(
            "Added delayed message {TaskId} for delivery at {DeliveryTime}",
            message.Id,
            deliveryTime
        );
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TaskMessage> GetDueMessagesAsync(
        DateTimeOffset now,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        // Use FOR UPDATE SKIP LOCKED to allow multiple workers to process due messages
        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE task_id IN (
                SELECT task_id FROM {_options.Schema}.{_options.TableName}
                WHERE delivery_time <= @now
                ORDER BY delivery_time
                LIMIT @limit
                FOR UPDATE SKIP LOCKED
            )
            RETURNING message
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("now", now.UtcDateTime);
        cmd.Parameters.AddWithValue("limit", _options.BatchSize);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var json = reader.GetString(0);
            var message = JsonSerializer.Deserialize(json, TaskMessageTypeInfo);
            if (message is not null)
            {
                yield return message;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> RemoveAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE task_id = @taskId
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("taskId", taskId);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        if (deleted > 0)
        {
            _logger.LogDebug("Removed delayed message {TaskId}", taskId);
        }

        return deleted > 0;
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT COUNT(*) FROM {_options.Schema}.{_options.TableName}
            """;

        await using var cmd = _dataSource.CreateCommand(sql);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <inheritdoc />
    public async ValueTask<DateTimeOffset?> GetNextDeliveryTimeAsync(
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sql = $"""
            SELECT MIN(delivery_time) FROM {_options.Schema}.{_options.TableName}
            """;

        await using var cmd = _dataSource.CreateCommand(sql);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (result is null or DBNull)
        {
            return null;
        }

        return new DateTimeOffset((DateTime)result, TimeSpan.Zero);
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
        _logger.LogInformation("PostgreSQL delayed message store disposed");
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
                task_id VARCHAR(255) PRIMARY KEY,
                message JSONB NOT NULL,
                delivery_time TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_{_options.TableName}_delivery_time
                ON {_options.Schema}.{_options.TableName} (delivery_time);
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("PostgreSQL delayed message store table created/verified");
    }
}
