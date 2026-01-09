using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Backend.Redis.Serialization;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace DotCelery.Backend.Redis;

/// <summary>
/// Redis result backend implementation using StackExchange.Redis.
/// </summary>
public sealed class RedisResultBackend : IResultBackend
{
    private readonly RedisBackendOptions _options;
    private readonly ILogger<RedisResultBackend> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<TaskResult>> _waiters =
        new();

    // AOT-friendly type info
    private static JsonTypeInfo<TaskResult> TaskResultTypeInfo =>
        RedisBackendJsonContext.Default.TaskResult;

    private ConnectionMultiplexer? _connection;
    private ISubscriber? _subscriber;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RedisResultBackend"/> class.
    /// </summary>
    /// <param name="options">The backend options.</param>
    /// <param name="logger">The logger.</param>
    public RedisResultBackend(
        IOptions<RedisBackendOptions> options,
        ILogger<RedisResultBackend> logger
    )
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask StoreResultAsync(
        TaskResult result,
        TimeSpan? expiry = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(result);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);

        var key = GetResultKey(result.TaskId);
        var stateKey = GetStateKey(result.TaskId);
        var json = JsonSerializer.Serialize(result, TaskResultTypeInfo);
        var effectiveExpiry = expiry ?? _options.DefaultExpiry;

        // Store result and state atomically using a transaction
        var transaction = db.CreateTransaction();

#pragma warning disable CA2012 // Use ValueTasks correctly - Redis transaction requires this pattern
        _ = transaction.StringSetAsync(key, json, effectiveExpiry);
        _ = transaction.StringSetAsync(stateKey, result.State.ToString(), effectiveExpiry);
#pragma warning restore CA2012

        await transaction.ExecuteAsync().ConfigureAwait(false);

        _logger.LogDebug(
            "Stored result for task {TaskId} with state {State}",
            result.TaskId,
            result.State
        );

        // Notify waiters via pub/sub
        if (_options.UsePubSub)
        {
            var subscriber = await GetSubscriberAsync(cancellationToken).ConfigureAwait(false);
            var channel = new RedisChannel(
                GetPubSubChannel(result.TaskId),
                RedisChannel.PatternMode.Literal
            );
            await subscriber.PublishAsync(channel, json).ConfigureAwait(false);
        }

        // Also notify local waiters
        if (_waiters.TryRemove(result.TaskId, out var tcs))
        {
            tcs.TrySetResult(result);
        }
    }

    /// <inheritdoc />
    public async ValueTask<TaskResult?> GetResultAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetResultKey(taskId);

        var json = await db.StringGetAsync(key).ConfigureAwait(false);
        if (json.IsNullOrEmpty)
        {
            return null;
        }

        return JsonSerializer.Deserialize((string)json!, TaskResultTypeInfo);
    }

    /// <inheritdoc />
    public async Task<TaskResult> WaitForResultAsync(
        string taskId,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        // Check if result already exists
        var existing = await GetResultAsync(taskId, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            return existing;
        }

        // Create or get existing waiter with RunContinuationsAsynchronously to prevent inline continuations
        var tcs = _waiters.GetOrAdd(
            taskId,
            _ => new TaskCompletionSource<TaskResult>(
                TaskCreationOptions.RunContinuationsAsynchronously
            )
        );

        // Check again after adding waiter (race condition)
        existing = await GetResultAsync(taskId, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
        {
            _waiters.TryRemove(taskId, out _);
            return existing;
        }

        // Subscribe to pub/sub notifications
        ChannelMessageQueue? subscription = null;
        if (_options.UsePubSub)
        {
            var subscriber = await GetSubscriberAsync(cancellationToken).ConfigureAwait(false);
            var channel = new RedisChannel(
                GetPubSubChannel(taskId),
                RedisChannel.PatternMode.Literal
            );
            subscription = await subscriber.SubscribeAsync(channel).ConfigureAwait(false);

            // Handle messages in background with proper exception handling
            _ = Task.Run(
                async () =>
                {
                    try
                    {
                        await foreach (
                            var message in subscription.WithCancellation(cancellationToken)
                        )
                        {
                            if (message.Message.HasValue)
                            {
                                var result = JsonSerializer.Deserialize(
                                    (string)message.Message!,
                                    TaskResultTypeInfo
                                );
                                if (
                                    result is not null
                                    && _waiters.TryRemove(taskId, out var waiter)
                                )
                                {
                                    waiter.TrySetResult(result);
                                }
                            }
                            break;
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected during cancellation
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Error in pub/sub message handler for task {TaskId}",
                            taskId
                        );
                        // Signal the waiter with the exception so it doesn't hang indefinitely
                        if (_waiters.TryRemove(taskId, out var waiter))
                        {
                            waiter.TrySetException(ex);
                        }
                    }
                },
                cancellationToken
            );
        }

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (timeout.HasValue)
            {
                cts.CancelAfter(timeout.Value);
            }

            await using var registration = cts.Token.Register(() => tcs.TrySetCanceled(cts.Token));

            // Also poll in case pub/sub notification is missed
            _ = Task.Run(
                async () =>
                {
                    try
                    {
                        while (!tcs.Task.IsCompleted && !cts.Token.IsCancellationRequested)
                        {
                            await Task.Delay(_options.PollingInterval, cts.Token)
                                .ConfigureAwait(false);

                            var result = await GetResultAsync(taskId, cts.Token)
                                .ConfigureAwait(false);
                            if (result is not null && _waiters.TryRemove(taskId, out var waiter))
                            {
                                waiter.TrySetResult(result);
                                break;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected during cancellation or timeout
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in polling handler for task {TaskId}", taskId);
                        // Signal the waiter with the exception so it doesn't hang indefinitely
                        if (_waiters.TryRemove(taskId, out var waiter))
                        {
                            waiter.TrySetException(ex);
                        }
                    }
                },
                cts.Token
            );

            return await tcs.Task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (timeout.HasValue && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Timeout waiting for task {taskId} result after {timeout.Value}"
            );
        }
        finally
        {
            // Always clean up waiter to prevent memory leaks
            _waiters.TryRemove(taskId, out _);

            if (subscription is not null)
            {
                await subscription.UnsubscribeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask UpdateStateAsync(
        string taskId,
        TaskState state,
        object? metadata = null,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetStateKey(taskId);

        await db.StringSetAsync(key, state.ToString(), _options.DefaultExpiry)
            .ConfigureAwait(false);

        _logger.LogDebug("Updated state for task {TaskId} to {State}", taskId, state);
    }

    /// <inheritdoc />
    public async ValueTask<TaskState?> GetStateAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var db = await GetDatabaseAsync(cancellationToken).ConfigureAwait(false);
        var key = GetStateKey(taskId);

        var value = await db.StringGetAsync(key).ConfigureAwait(false);
        if (value.IsNullOrEmpty)
        {
            return null;
        }

        return Enum.TryParse<TaskState>(value, ignoreCase: true, out var state) ? state : null;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // Cancel all waiters
        foreach (var tcs in _waiters.Values)
        {
            tcs.TrySetCanceled();
        }
        _waiters.Clear();

        if (_connection is not null)
        {
            await _connection.CloseAsync().ConfigureAwait(false);
            _connection.Dispose();
        }

        _connectionLock.Dispose();

        _logger.LogInformation("Redis backend disposed");
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
                "Connected to Redis at {ConnectionString}",
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

    private string GetResultKey(string taskId) => $"{_options.KeyPrefix}{taskId}";

    private string GetStateKey(string taskId) => $"{_options.StateKeyPrefix}{taskId}";

    private string GetPubSubChannel(string taskId) => $"{_options.PubSubChannelPrefix}{taskId}";
}
