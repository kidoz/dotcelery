using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace DotCelery.Backend.Postgres.Outbox;

/// <summary>
/// PostgreSQL implementation of <see cref="IInboxStore"/>.
/// Provides durable message deduplication for exactly-once processing.
/// </summary>
public sealed class PostgresInboxStore : IInboxStore
{
    private readonly PostgresInboxStoreOptions _options;
    private readonly ILogger<PostgresInboxStore> _logger;
    private readonly NpgsqlDataSource _dataSource;

    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresInboxStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="dataSources">Provides the shared PostgreSQL data source.</param>
    /// <param name="logger">The logger.</param>
    public PostgresInboxStore(
        IOptions<PostgresInboxStoreOptions> options,
        IPostgresDataSourceProvider dataSources,
        ILogger<PostgresInboxStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _dataSource = dataSources.GetDataSource(_options.ConnectionString);
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsProcessedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var sql = $"""
            SELECT EXISTS(
                SELECT 1 FROM {_options.Schema}.{_options.TableName}
                WHERE message_id = @messageId
            )
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("messageId", messageId);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is true;
    }

    /// <inheritdoc />
    public async ValueTask MarkProcessedAsync(
        string messageId,
        object? transaction = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var sql = $"""
            INSERT INTO {_options.Schema}.{_options.TableName} (message_id, processed_at)
            VALUES (@messageId, @processedAt)
            ON CONFLICT (message_id) DO NOTHING
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("messageId", messageId);
        cmd.Parameters.AddWithValue("processedAt", DateTime.UtcNow);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Marked message {MessageId} as processed", messageId);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var sql = $"SELECT COUNT(*) FROM {_options.Schema}.{_options.TableName}";

        await using var cmd = _dataSource.CreateCommand(sql);

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

        var cutoff = DateTime.UtcNow - olderThan;

        var sql = $"""
            DELETE FROM {_options.Schema}.{_options.TableName}
            WHERE processed_at < @cutoff
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue("cutoff", cutoff);

        var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("Cleaned up {Count} inbox records", deleted);
        return deleted;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return ValueTask.CompletedTask;
        }

        _disposed = true;

        _logger.LogInformation("PostgreSQL inbox store disposed");

        return ValueTask.CompletedTask;
    }
}
