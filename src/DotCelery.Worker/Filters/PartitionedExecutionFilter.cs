using DotCelery.Core.Filters;
using DotCelery.Core.Partitioning;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that ensures sequential processing for tasks with the same partition key.
/// Tasks without a partition key are processed without any locking.
/// </summary>
public sealed class PartitionedExecutionFilter : ITaskFilterWithExceptionHandling
{
    private readonly IPartitionLockStore _lockStore;
    private readonly PartitionOptions _options;
    private readonly ILogger<PartitionedExecutionFilter> _logger;

    private const string PartitionKeyProperty = "PartitionKey";
    private const string LockAcquiredProperty = "PartitionLockAcquired";

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionedExecutionFilter"/> class.
    /// </summary>
    public PartitionedExecutionFilter(
        IPartitionLockStore lockStore,
        IOptions<PartitionOptions> options,
        ILogger<PartitionedExecutionFilter> logger
    )
    {
        _lockStore = lockStore;
        _options = options.Value;
        _logger = logger;
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

        context.Properties[PartitionKeyProperty] = partitionKey;

        var acquired = await _lockStore
            .TryAcquireAsync(partitionKey, context.TaskId, _options.LockTimeout, cancellationToken)
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

        context.Properties[LockAcquiredProperty] = true;

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
        await ReleaseLockIfHeldAsync(context.TaskId, context.Properties, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> OnExceptionAsync(
        TaskExceptionContext context,
        CancellationToken cancellationToken
    )
    {
        await ReleaseLockIfHeldAsync(context.TaskId, context.Properties, cancellationToken)
            .ConfigureAwait(false);

        return false; // Don't handle the exception, let other filters/handlers deal with it
    }

    private async ValueTask ReleaseLockIfHeldAsync(
        string taskId,
        IDictionary<string, object?> properties,
        CancellationToken cancellationToken
    )
    {
        if (!properties.TryGetValue(LockAcquiredProperty, out var acquired) || acquired is not true)
        {
            return;
        }

        if (
            !properties.TryGetValue(PartitionKeyProperty, out var partitionKeyObj)
            || partitionKeyObj is not string partitionKey
        )
        {
            return;
        }

        var released = await _lockStore
            .ReleaseAsync(partitionKey, taskId, cancellationToken)
            .ConfigureAwait(false);

        if (released)
        {
            _logger.LogDebug(
                "Released partition lock for {PartitionKey}, task {TaskId}",
                partitionKey,
                taskId
            );
        }
    }
}

/// <summary>
/// Options for partitioned message processing.
/// </summary>
public sealed class PartitionOptions
{
    /// <summary>
    /// Gets or sets the lock timeout. Locks will auto-expire after this duration.
    /// Default is 30 minutes.
    /// </summary>
    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the delay before requeueing a message when partition is locked.
    /// Default is 1 second.
    /// </summary>
    public TimeSpan RequeueDelay { get; set; } = TimeSpan.FromSeconds(1);
}
