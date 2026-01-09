using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Outbox;

/// <summary>
/// Redis implementation of <see cref="IInboxStore"/>.
/// Uses a hash with timestamps for efficient deduplication and cleanup.
/// </summary>
public sealed class RedisInboxStore : IInboxStore
{
    private readonly RedisInboxStoreOptions _options;
    private readonly ILogger<RedisInboxStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisInboxStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    public RedisInboxStore(
        IOptions<RedisInboxStoreOptions> options,
        ILogger<RedisInboxStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsProcessedAsync(
        string messageId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(messageId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await db.HashExistsAsync(_options.ProcessedMessagesKey, messageId)
            .ConfigureAwait(false);
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

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await db.HashSetAsync(_options.ProcessedMessagesKey, messageId, timestamp)
            .ConfigureAwait(false);

        _logger.LogDebug("Marked message {MessageId} as processed", messageId);
    }

    /// <inheritdoc />
    public async ValueTask<long> GetCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await db.HashLengthAsync(_options.ProcessedMessagesKey).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupAsync(
        TimeSpan olderThan,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var cutoffTimestamp = (DateTimeOffset.UtcNow - olderThan).ToUnixTimeMilliseconds();
        long removed = 0;

        // Get all entries and check timestamps
        var entries = await db.HashGetAllAsync(_options.ProcessedMessagesKey).ConfigureAwait(false);

        var toRemove = new List<RedisValue>();

        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (entry.Value.TryParse(out long timestamp) && timestamp < cutoffTimestamp)
            {
                toRemove.Add(entry.Name);
            }
        }

        if (toRemove.Count > 0)
        {
            removed = await db.HashDeleteAsync(_options.ProcessedMessagesKey, [.. toRemove])
                .ConfigureAwait(false);
        }

        _logger.LogDebug("Cleaned up {Count} inbox records", removed);
        return removed;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection.Dispose();
        }

        _connectionLock.Dispose();

        _logger.LogInformation("Redis inbox store disposed");
    }

    private async Task<IConnectionMultiplexer> GetConnectionAsync(
        CancellationToken cancellationToken
    )
    {
        if (_connection?.IsConnected == true)
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_connection?.IsConnected == true)
            {
                return _connection;
            }

            var configOptions = ConfigurationOptions.Parse(_options.ConnectionString);
            configOptions.DefaultDatabase = _options.Database;
            configOptions.ConnectTimeout = (int)_options.ConnectTimeout.TotalMilliseconds;
            configOptions.SyncTimeout = (int)_options.SyncTimeout.TotalMilliseconds;
            configOptions.AbortOnConnectFail = _options.AbortOnConnectFail;

            _connection = await ConnectionMultiplexer
                .ConnectAsync(configOptions)
                .ConfigureAwait(false);
            _logger.LogInformation(
                "Connected to Redis for inbox store at {ConnectionString}",
                _options.ConnectionString
            );

            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task<IDatabase> GetDatabaseAsync(CancellationToken cancellationToken)
    {
        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        return connection.GetDatabase(_options.Database);
    }
}
