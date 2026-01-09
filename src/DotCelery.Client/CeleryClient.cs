using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Routing;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Client;

/// <summary>
/// Default implementation of <see cref="ICeleryClient"/>.
/// </summary>
public sealed class CeleryClient : ICeleryClient
{
    private readonly IMessageBroker _broker;
    private readonly IResultBackend _backend;
    private readonly IMessageSerializer _serializer;
    private readonly IRevocationStore? _revocationStore;
    private readonly IDelayedMessageStore? _delayedMessageStore;
    private readonly ITaskRouter? _taskRouter;
    private readonly ITaskSignalDispatcher _signalDispatcher;
    private readonly CeleryClientOptions _options;
    private readonly ILogger<CeleryClient> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="CeleryClient"/> class.
    /// </summary>
    /// <param name="broker">The message broker.</param>
    /// <param name="backend">The result backend.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The client options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="revocationStore">Optional revocation store for task cancellation.</param>
    /// <param name="signalDispatcher">Optional signal dispatcher for lifecycle events.</param>
    /// <param name="taskRouter">Optional task router for queue routing.</param>
    /// <param name="delayedMessageStore">Optional delayed message store for scheduled tasks.</param>
    public CeleryClient(
        IMessageBroker broker,
        IResultBackend backend,
        IMessageSerializer serializer,
        IOptions<CeleryClientOptions> options,
        ILogger<CeleryClient> logger,
        IRevocationStore? revocationStore = null,
        ITaskSignalDispatcher? signalDispatcher = null,
        ITaskRouter? taskRouter = null,
        IDelayedMessageStore? delayedMessageStore = null
    )
    {
        _broker = broker;
        _backend = backend;
        _serializer = serializer;
        _revocationStore = revocationStore;
        _delayedMessageStore = delayedMessageStore;
        _taskRouter = taskRouter;
        _signalDispatcher = signalDispatcher ?? NullTaskSignalDispatcher.Instance;
        _options = options.Value;
        _logger = logger;
    }

    // ========== Task Revocation ==========

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        if (_revocationStore is null)
        {
            throw new InvalidOperationException(
                "Revocation is not available. Register an IRevocationStore implementation to enable task revocation."
            );
        }

        await _revocationStore
            .RevokeAsync(taskId, options, cancellationToken)
            .ConfigureAwait(false);

        // Only update task state to Revoked if task is not already in a terminal state
        var currentState = await _backend
            .GetStateAsync(taskId, cancellationToken)
            .ConfigureAwait(false);
        if (
            currentState
            is not (
                TaskState.Success
                or TaskState.Failure
                or TaskState.Revoked
                or TaskState.Rejected
            )
        )
        {
            await _backend
                .UpdateStateAsync(taskId, TaskState.Revoked, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        _logger.LogInformation(
            "Task {TaskId} revoked with options: Terminate={Terminate}, Signal={Signal}",
            taskId,
            options?.Terminate ?? false,
            options?.Signal ?? CancellationSignal.Graceful
        );
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(taskIds);

        if (_revocationStore is null)
        {
            throw new InvalidOperationException(
                "Revocation is not available. Register an IRevocationStore implementation to enable task revocation."
            );
        }

        var taskIdList = taskIds.ToList();

        await _revocationStore
            .RevokeAsync(taskIdList, options, cancellationToken)
            .ConfigureAwait(false);

        // Update state for all tasks that are not already in a terminal state
        foreach (var taskId in taskIdList)
        {
            var currentState = await _backend
                .GetStateAsync(taskId, cancellationToken)
                .ConfigureAwait(false);
            if (
                currentState
                is not (
                    TaskState.Success
                    or TaskState.Failure
                    or TaskState.Revoked
                    or TaskState.Rejected
                )
            )
            {
                await _backend
                    .UpdateStateAsync(
                        taskId,
                        TaskState.Revoked,
                        cancellationToken: cancellationToken
                    )
                    .ConfigureAwait(false);
            }
        }

        _logger.LogInformation("Revoked {Count} tasks", taskIdList.Count);
    }

    /// <inheritdoc />
    public ValueTask<bool> IsRevokedAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        if (_revocationStore is null)
        {
            return ValueTask.FromResult(false);
        }

        return _revocationStore.IsRevokedAsync(taskId, cancellationToken);
    }

    // ========== Scheduled Task Management ==========

    /// <inheritdoc />
    public async ValueTask<bool> CancelScheduledAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        if (_delayedMessageStore is null)
        {
            _logger.LogWarning(
                "Cannot cancel scheduled task {TaskId}: No IDelayedMessageStore is registered",
                taskId
            );
            return false;
        }

        var removed = await _delayedMessageStore
            .RemoveAsync(taskId, cancellationToken)
            .ConfigureAwait(false);

        if (removed)
        {
            // Update task state to indicate it was cancelled
            await _backend
                .UpdateStateAsync(taskId, TaskState.Revoked, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation("Cancelled scheduled task {TaskId}", taskId);
        }
        else
        {
            _logger.LogDebug(
                "Scheduled task {TaskId} not found (may have already been dispatched)",
                taskId
            );
        }

        return removed;
    }

    // ========== Task Sending ==========

    /// <inheritdoc />
    public async ValueTask<AsyncResult<TOutput>> SendAsync<TTask, TInput, TOutput>(
        TInput input,
        SendOptions? options = null,
        CancellationToken cancellationToken = default
    )
        where TTask : ITask<TInput, TOutput>
        where TInput : class
        where TOutput : class
    {
        var taskId = await SendInternalAsync<TTask, TInput>(input, options, cancellationToken)
            .ConfigureAwait(false);
        return new AsyncResult<TOutput>(taskId, this, _serializer);
    }

    /// <inheritdoc />
    public async ValueTask<AsyncResult> SendAsync<TTask, TInput>(
        TInput input,
        SendOptions? options = null,
        CancellationToken cancellationToken = default
    )
        where TTask : ITask<TInput>
        where TInput : class
    {
        var taskId = await SendInternalAsync<TTask, TInput>(input, options, cancellationToken)
            .ConfigureAwait(false);
        return new AsyncResult(taskId, this);
    }

    /// <inheritdoc />
    public ValueTask<TaskResult?> GetResultAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskId);
        return _backend.GetResultAsync(taskId, cancellationToken);
    }

    /// <inheritdoc />
    public Task<TaskResult> WaitForResultAsync(
        string taskId,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(taskId);

        var effectiveTimeout = timeout ?? _options.DefaultTimeout;
        if (effectiveTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be positive.");
        }

        return _backend.WaitForResultAsync(taskId, effectiveTimeout, cancellationToken);
    }

    private async ValueTask<string> SendInternalAsync<TTask, TInput>(
        TInput input,
        SendOptions? options,
        CancellationToken cancellationToken
    )
        where TTask : ITask
        where TInput : class
    {
        ArgumentNullException.ThrowIfNull(input);

        var taskName = TTask.TaskName;
        var taskId = options?.TaskId ?? Guid.NewGuid().ToString("N");

        // Validate taskId format if provided explicitly
        if (options?.TaskId is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(options.TaskId, "options.TaskId");
        }

        // Validate priority range
        if (options?.Priority is < 0 or > 9)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "Priority must be between 0 and 9."
            );
        }

        // Validate ETA is in the future or reasonable past (within 5 minutes)
        if (options?.Eta.HasValue == true)
        {
            var now = DateTimeOffset.UtcNow;
            var minAllowedEta = now.AddMinutes(-5); // Allow some clock skew
            if (options.Eta.Value < minAllowedEta)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(options),
                    $"ETA cannot be more than 5 minutes in the past. ETA: {options.Eta.Value}, Now: {now}"
                );
            }
        }

        // Validate Countdown is positive
        if (options?.Countdown.HasValue == true && options.Countdown.Value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "Countdown must be a positive duration."
            );
        }

        // Validate MaxRetries is non-negative
        if (options?.MaxRetries.HasValue == true && options.MaxRetries.Value < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MaxRetries cannot be negative."
            );
        }

        // Determine queue: explicit option > router > default
        var queue =
            options?.Queue
            ?? _taskRouter?.GetQueue(taskName, _options.DefaultQueue)
            ?? _options.DefaultQueue;

        var eta = options?.Eta;
        if (options?.Countdown.HasValue == true)
        {
            eta = DateTimeOffset.UtcNow.Add(options.Countdown.Value);
        }

        var message = new TaskMessage
        {
            Id = taskId,
            Task = taskName,
            Args = _serializer.Serialize(input),
            ContentType = _serializer.ContentType,
            Timestamp = DateTimeOffset.UtcNow,
            Eta = eta,
            Expires = options?.Expires,
            MaxRetries = options?.MaxRetries ?? _options.DefaultMaxRetries,
            Priority = options?.Priority ?? 0,
            Queue = queue,
            CorrelationId = options?.CorrelationId,
            Headers = options?.Headers,
        };

        // Dispatch BeforeTaskPublish signal
        await _signalDispatcher
            .DispatchAsync(
                new BeforeTaskPublishSignal
                {
                    TaskId = taskId,
                    TaskName = taskName,
                    Timestamp = DateTimeOffset.UtcNow,
                    Input = input,
                    Queue = queue,
                    Eta = eta,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Sending task {TaskName} with ID {TaskId} to queue {Queue}",
            taskName,
            taskId,
            queue
        );

        await _broker.PublishAsync(message, cancellationToken).ConfigureAwait(false);
        await _backend
            .UpdateStateAsync(taskId, TaskState.Pending, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        // Dispatch AfterTaskPublish signal
        await _signalDispatcher
            .DispatchAsync(
                new AfterTaskPublishSignal
                {
                    TaskId = taskId,
                    TaskName = taskName,
                    Timestamp = DateTimeOffset.UtcNow,
                    Queue = queue,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogInformation("Task {TaskName} sent with ID {TaskId}", taskName, taskId);

        return taskId;
    }
}

/// <summary>
/// Options for the Celery client.
/// </summary>
public sealed class CeleryClientOptions
{
    /// <summary>
    /// Gets or sets the default queue name.
    /// </summary>
    public string DefaultQueue { get; set; } = "celery";

    /// <summary>
    /// Gets or sets the default timeout for waiting on results.
    /// </summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the default maximum retries.
    /// </summary>
    public int DefaultMaxRetries { get; set; } = 3;
}
