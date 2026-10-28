using System.Collections.Concurrent;
using DotCelery.Core.Filters;
using DotCelery.Core.Partitioning;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that ensures sequential processing for tasks with the same partition key.
/// Tasks without a partition key are processed without any locking.
/// </summary>
/// <remarks>
/// Lease loss cancels the task token and prevents successful ordered completion. Tasks must
/// observe cancellation; external effects requiring strict fencing must validate ownership too.
/// </remarks>
public sealed class PartitionedExecutionFilter
    : ITaskFilterWithExceptionHandling,
        IExecutionResourceFilter,
        IAsyncDisposable
{
    private readonly IPartitionLockStore _lockStore;
    private readonly PartitionOptions _options;
    private readonly ILogger<PartitionedExecutionFilter> _logger;

    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<PartitionLease, byte> _leases = new();
    private const string LeaseProperty = "PartitionExecutionLease";

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionedExecutionFilter"/> class.
    /// </summary>
    public PartitionedExecutionFilter(
        IPartitionLockStore lockStore,
        IOptions<PartitionOptions> options,
        ILogger<PartitionedExecutionFilter> logger
    )
        : this(lockStore, options, logger, TimeProvider.System) { }

    /// <summary>Initializes the filter with the clock used to renew partition leases.</summary>
    /// <param name="lockStore">The partition lock store.</param>
    /// <param name="options">The partition options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">The renewal clock.</param>
    public PartitionedExecutionFilter(
        IPartitionLockStore lockStore,
        IOptions<PartitionOptions> options,
        ILogger<PartitionedExecutionFilter> logger,
        TimeProvider timeProvider
    )
    {
        _timeProvider = timeProvider;
        _lockStore = lockStore;
        _options = options.Value;
        _logger = logger;
        ArgumentOutOfRangeException.ThrowIfLessThan(
            _options.LockTimeout,
            TimeSpan.FromMilliseconds(3)
        );
        ArgumentOutOfRangeException.ThrowIfLessThan(_options.RequeueDelay, TimeSpan.Zero);
    }

    /// <inheritdoc />
    public int Order => -1000; // Run very early to acquire lock before other filters

    /// <inheritdoc />
    public async ValueTask OnExecutingAsync(
        TaskExecutingContext context,
        CancellationToken cancellationToken
    )
    {
        var partitionKey = context.TaskContext.PartitionKey;

        if (string.IsNullOrEmpty(partitionKey))
        {
            return; // No partition key, no locking needed
        }

        var owner = $"{context.TaskId}:{Guid.NewGuid():N}";

        var acquired = await _lockStore
            .TryAcquireAsync(partitionKey, owner, _options.LockTimeout, cancellationToken)
            .ConfigureAwait(false);

        if (!acquired)
        {
            _logger.LogDebug(
                "Partition {PartitionKey} is locked, requeueing task {TaskId} with delay {RequeueDelay}",
                partitionKey,
                context.TaskId,
                _options.RequeueDelay
            );

            // Skip execution and signal that message should be requeued with delay
            context.SkipExecution = true;
            context.RequeueMessage = true;
            context.RequeueDelay = _options.RequeueDelay;
            return;
        }

        var lease = new PartitionLease(
            _lockStore,
            partitionKey,
            owner,
            _options.LockTimeout,
            _timeProvider
        );
        _leases.TryAdd(lease, 0);
        context.Properties[LeaseProperty] = lease;

        _logger.LogDebug(
            "Acquired partition lock for {PartitionKey}, task {TaskId}",
            partitionKey,
            context.TaskId
        );
    }

    /// <inheritdoc />
    public async ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await ReleaseLockIfHeldAsync(context.Properties).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.Exception = ex;
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> OnExceptionAsync(
        TaskExceptionContext context,
        CancellationToken cancellationToken
    )
    {
        // Completion reports lease loss even when an exception filter handles the task error.
        return ValueTask.FromResult(false);
    }

    internal static CancellationToken LeaseLostToken(TaskExecutingContext context) =>
        context.Properties.TryGetValue(LeaseProperty, out var value)
        && value is PartitionLease lease
            ? lease.LostToken
            : CancellationToken.None;

    private async ValueTask ReleaseLockIfHeldAsync(IDictionary<string, object?> properties)
    {
        if (
            properties.TryGetValue(LeaseProperty, out var value)
            && value is PartitionLease lease
            && _leases.TryRemove(lease, out _)
        )
            await lease.DisposeAsync().ConfigureAwait(false);
    }

    ValueTask IExecutionResourceFilter.ReleaseAsync(TaskExecutingContext context) =>
        ReleaseLockIfHeldAsync(context.Properties);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (var lease in _leases.Keys)
            if (_leases.TryRemove(lease, out _))
                await lease.DisposeAsync().ConfigureAwait(false);
    }

    private sealed class PartitionLease : IAsyncDisposable
    {
        private readonly IPartitionLockStore _store;
        private readonly string _key;
        private readonly string _owner;
        private readonly CancellationTokenSource _stop = new();
        private readonly CancellationTokenSource _lost = new();
        private readonly Task _renewal;
        private readonly TimeSpan _timeout;
        private readonly TimeProvider _clock;
        private Exception? _failure;

        public PartitionLease(
            IPartitionLockStore store,
            string key,
            string owner,
            TimeSpan timeout,
            TimeProvider clock
        )
        {
            _store = store;
            _key = key;
            _owner = owner;
            _timeout = timeout;
            _clock = clock;
            _renewal = RenewAsync(timeout, clock);
        }

        public CancellationToken LostToken => _lost.Token;

        private async Task RenewAsync(TimeSpan timeout, TimeProvider clock)
        {
            try
            {
                while (true)
                {
                    await Task.Delay(timeout / 3, clock, _stop.Token).ConfigureAwait(false);
                    if (
                        !await _store
                            .ExtendAsync(_key, _owner, timeout, _stop.Token)
                            .AsTask()
                            .WaitAsync(timeout / 3, clock, _stop.Token)
                            .ConfigureAwait(false)
                    )
                        throw new InvalidOperationException(
                            $"Partition lease for {_key} was lost."
                        );
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _failure = new InvalidOperationException(
                    $"Partition lease for {_key} could not be renewed.",
                    ex
                );
                await _lost.CancelAsync().ConfigureAwait(false);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            try
            {
                await _renewal.ConfigureAwait(false);
                // A final renewal verifies ownership before an ordered success is accepted.
                if (_failure is not null)
                    throw _failure;
                if (
                    !await _store
                        .ExtendAsync(_key, _owner, _timeout, CancellationToken.None)
                        .AsTask()
                        .WaitAsync(_timeout / 3, _clock)
                        .ConfigureAwait(false)
                )
                    throw new InvalidOperationException($"Partition lease for {_key} was lost.");
            }
            finally
            {
                _stop.Dispose();
                _lost.Dispose();
                await _store
                    .ReleaseAsync(_key, _owner, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }
    }
}

/// <summary>
/// Options for partitioned message processing.
/// </summary>
public sealed class PartitionOptions
{
    /// <summary>
    /// Gets or sets the lock timeout. Leases are renewed while execution runs and expire after this duration if renewal stops.
    /// Default is 30 minutes.
    /// </summary>
    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the delay before requeueing a message when partition is locked.
    /// Default is 1 second.
    /// </summary>
    public TimeSpan RequeueDelay { get; set; } = TimeSpan.FromSeconds(1);
}
