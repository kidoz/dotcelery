using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Channels;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis.Revocation;

/// <summary>
/// Redis implementation of revocation store using sets and pub/sub.
/// Provides distributed task revocation with real-time notifications.
/// </summary>
public sealed class RedisRevocationStore : IRevocationStore
{
    private readonly RedisRevocationStoreOptions _options;
    private readonly ILogger<RedisRevocationStore> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly Channel<RevocationEvent> _localEventChannel;

    // AOT-friendly type info
    private static JsonTypeInfo<RevocationEntry> RevocationEntryTypeInfo =>
        RedisBackendJsonContext.Default.RevocationEntry;

    private static JsonTypeInfo<RevocationEvent> RevocationEventTypeInfo =>
        DotCeleryJsonContext.Default.RevocationEvent;

    private ConnectionMultiplexer? _connection;
    private ISubscriber? _subscriber;
    private ChannelMessageQueue? _subscription;
    private CancellationTokenSource? _subscriptionCts;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisRevocationStore"/> class.
    /// </summary>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    public RedisRevocationStore(
        IOptions<RedisRevocationStoreOptions> options,
        ILogger<RedisRevocationStore> logger
    )
    {
        _options = options.Value;
        _logger = logger;
        _localEventChannel = Channel.CreateUnbounded<RevocationEvent>(
            new UnboundedChannelOptions { SingleReader = false, SingleWriter = false }
        );
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        options ??= RevokeOptions.Default;
        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var entry = new RevocationEntry
        {
            TaskId = taskId,
            Options = options,
            RevokedAt = now,
            ExpiresAt = options.Expiry.HasValue ? now.Add(options.Expiry.Value) : null,
        };

        var json = JsonSerializer.Serialize(entry, RevocationEntryTypeInfo);

        // Store in hash with optional expiry
        await db.HashSetAsync(_options.RevocationsKey, taskId, json).ConfigureAwait(false);

        // Set expiry on the individual entry if specified
        if (options.Expiry.HasValue)
        {
            // Store expiry info in a separate sorted set for cleanup
            var expiryScore = entry.ExpiresAt!.Value.ToUnixTimeMilliseconds();
            await db.SortedSetAddAsync(_options.RevocationExpiryKey, taskId, expiryScore)
                .ConfigureAwait(false);
        }

        _logger.LogDebug("Revoked task {TaskId} with options {@Options}", taskId, options);

        // Publish revocation event
        var evt = new RevocationEvent
        {
            TaskId = taskId,
            Options = options,
            Timestamp = now,
        };

        var evtJson = JsonSerializer.Serialize(evt, RevocationEventTypeInfo);
        var subscriber = await GetSubscriberAsync(cancellationToken).ConfigureAwait(false);
        var channel = new RedisChannel(
            _options.RevocationChannel,
            RedisChannel.PatternMode.Literal
        );
        await subscriber.PublishAsync(channel, evtJson).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(taskIds);

        foreach (var taskId in taskIds)
        {
            await RevokeAsync(taskId, options, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsRevokedAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        var json = await db.HashGetAsync(_options.RevocationsKey, taskId).ConfigureAwait(false);
        if (json.IsNullOrEmpty)
        {
            return false;
        }

        var entry = JsonSerializer.Deserialize((string)json!, RevocationEntryTypeInfo);
        if (entry is null)
        {
            return false;
        }

        // Check if expired
        if (entry.ExpiresAt.HasValue && entry.ExpiresAt.Value < DateTimeOffset.UtcNow)
        {
            // Remove expired entry
            await RemoveExpiredEntryAsync(db, taskId).ConfigureAwait(false);
            return false;
        }

        return true;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> GetRevokedTaskIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;

        // Get all entries from the hash
        var entries = await db.HashGetAllAsync(_options.RevocationsKey).ConfigureAwait(false);

        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var revocation = JsonSerializer.Deserialize(
                (string)entry.Value!,
                RevocationEntryTypeInfo
            );
            if (revocation is null)
            {
                continue;
            }

            // Skip expired entries
            if (revocation.ExpiresAt.HasValue && revocation.ExpiresAt.Value < now)
            {
                // Remove expired entry in background
                _ = RemoveExpiredEntryAsync(db, revocation.TaskId);
                continue;
            }

            yield return revocation.TaskId;
        }
    }

    /// <inheritdoc />
    public async ValueTask<long> CleanupAsync(
        TimeSpan maxAge,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var cutoffTime = DateTimeOffset.UtcNow - maxAge;
        var cutoffScore = cutoffTime.ToUnixTimeMilliseconds();
        long removed = 0;

        // Get expired entries from the expiry sorted set
        var expiredTaskIds = await db.SortedSetRangeByScoreAsync(
                _options.RevocationExpiryKey,
                double.NegativeInfinity,
                cutoffScore
            )
            .ConfigureAwait(false);

        foreach (var taskIdValue in expiredTaskIds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var taskId = (string)taskIdValue!;
            await RemoveExpiredEntryAsync(db, taskId).ConfigureAwait(false);
            removed++;
        }

        // Also check entries by age (for entries without explicit expiry)
        var allEntries = await db.HashGetAllAsync(_options.RevocationsKey).ConfigureAwait(false);

        foreach (var entry in allEntries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var revocation = JsonSerializer.Deserialize(
                (string)entry.Value!,
                RevocationEntryTypeInfo
            );
            if (revocation is null)
            {
                continue;
            }

            // Remove if older than maxAge
            if (revocation.RevokedAt < cutoffTime)
            {
                await db.HashDeleteAsync(_options.RevocationsKey, revocation.TaskId)
                    .ConfigureAwait(false);
                removed++;
            }
        }

        _logger.LogDebug("Cleaned up {Count} expired revocations", removed);
        return removed;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RevocationEvent> SubscribeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await EnsureSubscribedAsync(cancellationToken).ConfigureAwait(false);

        await foreach (
            var evt in _localEventChannel
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            yield return evt;
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        _subscriptionCts?.Cancel();
        _subscriptionCts?.Dispose();

        if (_subscription is not null)
        {
            await _subscription.UnsubscribeAsync().ConfigureAwait(false);
        }

        _localEventChannel.Writer.Complete();

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection.Dispose();
        }

        _connectionLock.Dispose();

        _logger.LogInformation("Redis revocation store disposed");
    }

    private async Task EnsureSubscribedAsync(CancellationToken cancellationToken)
    {
        if (_subscription is not null)
        {
            return;
        }

        var subscriber = await GetSubscriberAsync(cancellationToken).ConfigureAwait(false);
        var channel = new RedisChannel(
            _options.RevocationChannel,
            RedisChannel.PatternMode.Literal
        );
        _subscription = await subscriber.SubscribeAsync(channel).ConfigureAwait(false);

        _subscriptionCts = new CancellationTokenSource();

        // Process messages in background
        _ = Task.Run(
            async () =>
            {
                try
                {
                    await foreach (
                        var message in _subscription.WithCancellation(_subscriptionCts.Token)
                    )
                    {
                        if (message.Message.HasValue)
                        {
                            try
                            {
                                var evt = JsonSerializer.Deserialize(
                                    (string)message.Message!,
                                    RevocationEventTypeInfo
                                );

                                if (evt is not null)
                                {
                                    await _localEventChannel
                                        .Writer.WriteAsync(evt, _subscriptionCts.Token)
                                        .ConfigureAwait(false);
                                }
                            }
                            catch (JsonException ex)
                            {
                                _logger.LogWarning(ex, "Failed to deserialize revocation event");
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Expected during disposal
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in revocation subscription");
                }
            },
            _subscriptionCts.Token
        );
    }

    private async Task RemoveExpiredEntryAsync(IDatabase db, string taskId)
    {
        var transaction = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
        _ = transaction.HashDeleteAsync(_options.RevocationsKey, taskId);
        _ = transaction.SortedSetRemoveAsync(_options.RevocationExpiryKey, taskId);
#pragma warning restore CA2012

        await transaction.ExecuteAsync().ConfigureAwait(false);
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
                "Connected to Redis for revocation store at {ConnectionString}",
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

    private async Task<ISubscriber> GetSubscriberAsync(CancellationToken cancellationToken)
    {
        if (_subscriber is not null)
        {
            return _subscriber;
        }

        var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        _subscriber = connection.GetSubscriber();
        return _subscriber;
    }
}
