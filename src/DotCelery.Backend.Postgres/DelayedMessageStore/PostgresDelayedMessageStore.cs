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

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresDelayedMessageStore"/> class.
    /// </summary>
    public PostgresDelayedMessageStore(
        IOptions<PostgresDelayedMessageStoreOptions> options,
        IPostgresDataSourceProvider dataSources,
        ILogger<PostgresDelayedMessageStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = dataSources.GetDataSource(_options.ConnectionString);
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
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;
        _logger.LogInformation("PostgreSQL delayed message store disposed");

        return ValueTask.CompletedTask;
    }
}
