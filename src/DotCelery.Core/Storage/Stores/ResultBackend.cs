using System.Collections.Concurrent;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IResultBackend"/> on the storage primitives.
/// </summary>
/// <remarks>
/// <para>
/// Each task has one document with its state and its last stored result. A state update never
/// replaces a final state (success, failure, revoked, or rejected), and <see cref="TaskState.Pending"/>
/// is recorded only for a task without a state, so a client that records a task as pending
/// after a worker has picked it up does not move the task back. Stored results always replace
/// the state. The <c>metadata</c> of <see cref="UpdateStateAsync"/> is not stored.
/// </para>
/// <para>
/// <see cref="WaitForResultAsync"/> returns once a result with a final state is stored. Waiters
/// are woken by the provider's notifications when it has them, and otherwise check every
/// <see cref="StorageStoreOptions.ResultPollInterval"/>.
/// </para>
/// </remarks>
public sealed class ResultBackend : IResultBackend
{
    private static readonly JsonTypeInfo<TaskRecord> RecordTypeInfo =
        StoreJson.TypeInfo<TaskRecord>();

    private readonly IDocumentStore _documents;
    private readonly INotificationChannel? _notifications;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ResultBackend> _logger;
    private readonly string _collection;
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _signals = new(
        StringComparer.Ordinal
    );
    private readonly CancellationTokenSource _disposed = new();
    private readonly Lock _listenerLock = new();
    private Task? _listener;

    /// <summary>
    /// Initializes a new instance of the <see cref="ResultBackend"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for timeouts and polling.</param>
    /// <param name="logger">The logger.</param>
    public ResultBackend(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null,
        ILogger<ResultBackend>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _documents = storage.Documents;
        _notifications = storage.Notifications;
        _options = options?.Value ?? new StorageStoreOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<ResultBackend>.Instance;
        _collection = _options.Name("results");
    }

    /// <inheritdoc />
    public async ValueTask StoreResultAsync(
        TaskResult result,
        TimeSpan? expiry = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(result);

        await _documents
            .UpsertAsync(
                _collection,
                result.TaskId,
                new TaskRecord(result.State, result),
                RecordTypeInfo,
                WriteOptions(expiry),
                cancellationToken
            )
            .ConfigureAwait(false);

        if (_signals.TryRemove(result.TaskId, out var signal))
        {
            signal.TrySetResult();
        }

        if (_notifications is not null)
        {
            await _notifications
                .PublishAsync(_collection, result.TaskId, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<TaskResult?> GetResultAsync(
        string taskId,
        CancellationToken cancellationToken = default
    ) => (await ReadAsync(taskId, cancellationToken).ConfigureAwait(false))?.Result;

    /// <inheritdoc />
    public async Task<TaskResult> WaitForResultAsync(
        string taskId,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        using var timeoutCts = timeout is { } delay
            ? new CancellationTokenSource(delay, _timeProvider)
            : new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token,
            _disposed.Token
        );
        EnsureListening();

        TaskCompletionSource? signal = null;
        try
        {
            while (true)
            {
                // Registered before reading, so a result stored after the read wakes this waiter
                signal = _signals.GetOrAdd(
                    taskId,
                    _ => new TaskCompletionSource(
                        TaskCreationOptions.RunContinuationsAsynchronously
                    )
                );

                var record = await ReadAsync(taskId, linked.Token).ConfigureAwait(false);
                if (record?.Result is { } result && IsFinal(result.State))
                {
                    return result;
                }

                try
                {
                    await signal
                        .Task.WaitAsync(_options.ResultPollInterval, _timeProvider, linked.Token)
                        .ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    // Check again in case a notification was missed
                }
            }
        }
        catch (OperationCanceledException)
            when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Timed out after {timeout} waiting for the result of task {taskId}."
            );
        }
        finally
        {
            if (signal is not null)
            {
                _signals.TryRemove(KeyValuePair.Create(taskId, signal));
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
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        while (true)
        {
            var inserted = await _documents
                .TryInsertAsync(
                    _collection,
                    taskId,
                    new TaskRecord(state, null),
                    RecordTypeInfo,
                    WriteOptions(null),
                    cancellationToken
                )
                .ConfigureAwait(false);
            if (inserted is not null)
            {
                return;
            }

            var updated = await _documents
                .UpdateAsync(
                    _collection,
                    taskId,
                    RecordTypeInfo,
                    current =>
                        IsFinal(current.Value.State) || state == TaskState.Pending
                            ? null
                            : current.Value with
                            {
                                State = state,
                            },
                    _ => WriteOptions(null),
                    cancellationToken
                )
                .ConfigureAwait(false);

            // Otherwise the task expired or was removed in between, so insert it again
            if (updated is not null)
            {
                return;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask<TaskState?> GetStateAsync(
        string taskId,
        CancellationToken cancellationToken = default
    ) => (await ReadAsync(taskId, cancellationToken).ConfigureAwait(false))?.State;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed.IsCancellationRequested)
        {
            return;
        }

        await _disposed.CancelAsync().ConfigureAwait(false);
        if (_listener is not null)
        {
            await _listener.ConfigureAwait(false);
        }

        _disposed.Dispose();
    }

    private static bool IsFinal(TaskState state) =>
        state is TaskState.Success or TaskState.Failure or TaskState.Revoked or TaskState.Rejected;

    private DocumentWriteOptions WriteOptions(TimeSpan? expiry) =>
        new() { TimeToLive = expiry ?? _options.ResultExpiry };

    private async ValueTask<TaskRecord?> ReadAsync(
        string taskId,
        CancellationToken cancellationToken
    ) =>
        (
            await _documents
                .GetAsync(_collection, taskId, RecordTypeInfo, cancellationToken)
                .ConfigureAwait(false)
        )?.Value;

    private void EnsureListening()
    {
        if (_notifications is null || _listener is not null)
        {
            return;
        }

        lock (_listenerLock)
        {
            _listener ??= ListenAsync(_notifications, _disposed.Token);
        }
    }

    // One subscription wakes every waiter in this process
    private async Task ListenAsync(
        INotificationChannel notifications,
        CancellationToken cancellationToken
    )
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await foreach (
                    var taskId in notifications
                        .SubscribeAsync(_collection, cancellationToken)
                        .ConfigureAwait(false)
                )
                {
                    if (_signals.TryRemove(taskId, out var signal))
                    {
                        signal.TrySetResult();
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Result notifications failed; waiters poll until resubscribed"
                );
            }

            try
            {
                await Task.Delay(_options.ResultPollInterval, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    /// The stored state of a task and its last stored result.
    /// </summary>
    internal sealed record TaskRecord(TaskState State, TaskResult? Result);
}
