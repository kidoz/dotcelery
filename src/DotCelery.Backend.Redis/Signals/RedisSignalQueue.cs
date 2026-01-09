using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Signals;

/// <summary>
/// Redis implementation of <see cref="ISignalStore"/>.
/// Uses a list for the queue and a hash for processing messages.
/// </summary>
public sealed class RedisSignalStore : ISignalStore
{
    private readonly RedisSignalStoreOptions _options;
    private readonly ILogger<RedisSignalStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);

    // AOT-friendly type info
    private static JsonTypeInfo<SignalMessage> SignalMessageTypeInfo =>
        RedisBackendJsonContext.Default.SignalMessage;

    private ConnectionMultiplexer? _connection;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisSignalStore"/> class.
    /// </summary>
    /// <param name="options">The queue options.</param>
    /// <param name="logger">The logger.</param>
    public RedisSignalStore(
        IOptions<RedisSignalStoreOptions> options,
        ILogger<RedisSignalStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask EnqueueAsync(
        SignalMessage message,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(message);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var json = JsonSerializer.Serialize(message, SignalMessageTypeInfo);

        await db.ListRightPushAsync(_options.PendingQueueKey, json).ConfigureAwait(false);

        _logger.LogDebug(
            "Enqueued signal message {MessageId} for task {TaskId}",
            message.Id,
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

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var count = 0;

        while (count < batchSize)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            // Pop from pending queue
            var json = await db.ListLeftPopAsync(_options.PendingQueueKey).ConfigureAwait(false);

            if (json.IsNullOrEmpty)
            {
                yield break;
            }

            var message = JsonSerializer.Deserialize((string)json!, SignalMessageTypeInfo);

            if (message is null)
            {
                _logger.LogWarning("Failed to deserialize signal message from queue");
                continue;
            }

            // Track as processing with timestamp for visibility timeout
            var processingValue = $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}|{json}";
            await db.HashSetAsync(_options.ProcessingKey, message.Id, processingValue)
                .ConfigureAwait(false);

            count++;
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

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        await db.HashDeleteAsync(_options.ProcessingKey, messageId).ConfigureAwait(false);

        _logger.LogDebug("Acknowledged signal message {MessageId}", messageId);
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

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        // Get the processing entry
        var value = await db.HashGetAsync(_options.ProcessingKey, messageId).ConfigureAwait(false);

        if (value.IsNullOrEmpty)
        {
            return;
        }

        // Remove from processing
        await db.HashDeleteAsync(_options.ProcessingKey, messageId).ConfigureAwait(false);

        if (requeue)
        {
            // Extract the original JSON and requeue
            var parts = ((string)value!).Split('|', 2);
            if (parts.Length == 2)
            {
                await db.ListRightPushAsync(_options.PendingQueueKey, parts[1])
                    .ConfigureAwait(false);
                _logger.LogDebug("Requeued signal message {MessageId}", messageId);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        return await db.ListLengthAsync(_options.PendingQueueKey).ConfigureAwait(false);
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

        _logger.LogInformation("Redis signal queue disposed");
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
                "Connected to Redis for signal queue at {ConnectionString}",
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
