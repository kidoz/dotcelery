using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Models;
using DotCelery.Worker.Execution;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker;

/// <summary>
/// Background service that runs the Celery worker.
/// </summary>
/// <remarks>
/// <para>
/// A message is acknowledged only after its task outcome is stored, so delivery is
/// at-least-once: a task can run again if the worker stops or a dependency fails first.
/// </para>
/// <para>
/// Messages for unknown tasks are rejected without requeue. After an infrastructure failure,
/// such as an unavailable result backend, the message is returned to the broker once
/// <see cref="WorkerOptions.InfrastructureFailureRequeueDelay"/> has elapsed.
/// </para>
/// </remarks>
public sealed class CeleryWorkerService : BackgroundService
{
    private readonly IMessageBroker _broker;
    private readonly TaskExecutor _executor;
    private readonly IDelayedMessageStore? _delayedMessageStore;
    private readonly IGracefulShutdownHandler? _shutdownHandler;
    private readonly IKillSwitch? _killSwitch;
    private readonly WorkerOptions _options;
    private readonly ILogger<CeleryWorkerService> _logger;
    private readonly Channel<BrokerMessage> _workChannel;
    private readonly string _workerName;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationTokenSource _stopIntakeCts = new();
    private readonly CancellationTokenSource _abortExecutionCts = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="CeleryWorkerService"/> class.
    /// </summary>
    public CeleryWorkerService(
        IMessageBroker broker,
        TaskExecutor executor,
        IOptions<WorkerOptions> options,
        ILogger<CeleryWorkerService> logger,
        IDelayedMessageStore? delayedMessageStore = null,
        IGracefulShutdownHandler? shutdownHandler = null,
        IKillSwitch? killSwitch = null,
        TimeProvider? timeProvider = null
    )
    {
        _broker = broker;
        _executor = executor;
        _delayedMessageStore = delayedMessageStore;
        _shutdownHandler = shutdownHandler;
        _killSwitch = killSwitch;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _workerName =
            _options.WorkerName ?? $"worker-{Environment.MachineName}-{Environment.ProcessId}";

        _workChannel = Channel.CreateBounded<BrokerMessage>(
            new BoundedChannelOptions(_options.PrefetchCount * _options.Concurrency)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = false,
                SingleWriter = true,
            }
        );
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Starting DotCelery worker {WorkerName} with concurrency {Concurrency}, queues: {Queues}",
            _workerName,
            _options.Concurrency,
            string.Join(", ", _options.Queues)
        );

        using var intakeCts = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken,
            _stopIntakeCts.Token
        );
        using var executionCts = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken,
            _abortExecutionCts.Token
        );

        // Not linked to stoppingToken: the broker consumer is closed only after every worker
        // has settled its message, because brokers such as RabbitMQ requeue all unsettled
        // messages when a consumer closes, including messages whose tasks are still running.
        using var consumeCts = new CancellationTokenSource();

        // Start worker tasks
        var workers = Enumerable
            .Range(0, _options.Concurrency)
            .Select(i => ProcessMessagesAsync(i, intakeCts.Token, executionCts.Token))
            .ToList();

        var consumer = _broker
            .ConsumeAsync(_options.Queues.ToList(), consumeCts.Token)
            .GetAsyncEnumerator(CancellationToken.None);

        Task<bool>? pendingRead = null;
        Exception? consumeFailure = null;

        try
        {
            pendingRead = await PumpMessagesAsync(consumer, intakeCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            consumeFailure = ex;
        }
        finally
        {
            _workChannel.Writer.Complete();
        }

        await Task.WhenAll(workers).ConfigureAwait(false);
        await CloseConsumerAsync(consumer, consumeCts, pendingRead).ConfigureAwait(false);

        if (intakeCts.IsCancellationRequested)
        {
            if (consumeFailure is not null)
            {
                _logger.LogWarning(
                    consumeFailure,
                    "Worker {WorkerName} consumer failed while stopping",
                    _workerName
                );
            }

            _logger.LogInformation("Worker {WorkerName} stopped", _workerName);
            return;
        }

        // The broker stopped delivering while the worker was running. Fail the service so the
        // host stops (or its supervisor restarts it) instead of running without a consumer.
        if (consumeFailure is not null)
        {
            _logger.LogError(
                consumeFailure,
                "Worker {WorkerName} stopped because consuming from the broker failed",
                _workerName
            );
            ExceptionDispatchInfo.Capture(consumeFailure).Throw();
        }

        _logger.LogError(
            "Worker {WorkerName} stopped because the broker ended the message stream",
            _workerName
        );
        throw new InvalidOperationException(
            $"Worker {_workerName} stopped because the broker ended the message stream."
        );
    }

    /// <summary>
    /// Moves messages from the broker to the workers until intake stops or the broker ends
    /// the stream.
    /// </summary>
    /// <returns>A broker read that was still in progress when intake stopped, if any.</returns>
    private async Task<Task<bool>?> PumpMessagesAsync(
        IAsyncEnumerator<BrokerMessage> consumer,
        CancellationToken intakeToken
    )
    {
        while (!intakeToken.IsCancellationRequested)
        {
            // Stopping intake must not cancel the broker read itself: that would close the
            // consumer while tasks are still running.
            var read = consumer.MoveNextAsync().AsTask();

            try
            {
                if (!await read.WaitAsync(intakeToken).ConfigureAwait(false))
                {
                    return null;
                }
            }
            catch (OperationCanceledException) when (intakeToken.IsCancellationRequested)
            {
                return read;
            }

            await DispatchAsync(consumer.Current, intakeToken).ConfigureAwait(false);
        }

        return null;
    }

    private async Task DispatchAsync(BrokerMessage message, CancellationToken intakeToken)
    {
        try
        {
            // Wait if kill switch is tripped
            if (_killSwitch is not null)
            {
                await _killSwitch.WaitUntilReadyAsync(intakeToken).ConfigureAwait(false);
            }

            var now = _timeProvider.GetUtcNow();

            // Check if task has expired
            if (message.Message.Expires.HasValue && message.Message.Expires.Value < now)
            {
                _logger.LogWarning("Task {TaskId} has expired, skipping", message.Message.Id);
                await AckAsync(message).ConfigureAwait(false);
                return;
            }

            // Check if ETA is in the future
            if (message.Message.Eta.HasValue && message.Message.Eta.Value > now)
            {
                if (_options.UseDelayQueue && _delayedMessageStore is not null)
                {
                    // Add to delay store for efficient handling
                    await _delayedMessageStore
                        .AddAsync(message.Message, message.Message.Eta.Value, intakeToken)
                        .ConfigureAwait(false);

                    await AckAsync(message).ConfigureAwait(false);

                    _logger.LogDebug(
                        "Task {TaskId} scheduled for {Eta} via delay store",
                        message.Message.Id,
                        message.Message.Eta.Value
                    );
                }
                else
                {
                    // Fallback behavior: wait until ETA or a maximum delay to prevent spin loop
                    var delayUntilEta = message.Message.Eta.Value - now;
                    var maxFallbackDelay = TimeSpan.FromSeconds(5);
                    var actualDelay =
                        delayUntilEta < maxFallbackDelay ? delayUntilEta : maxFallbackDelay;

                    _logger.LogDebug(
                        "Task {TaskId} has future ETA {Eta}, waiting {Delay} before requeue (no delay store configured)",
                        message.Message.Id,
                        message.Message.Eta.Value,
                        actualDelay
                    );

                    await Task.Delay(actualDelay, intakeToken).ConfigureAwait(false);
                    await ReturnToBrokerAsync(message).ConfigureAwait(false);
                }

                return;
            }

            await _workChannel.Writer.WriteAsync(message, intakeToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (intakeToken.IsCancellationRequested)
        {
            // Intake stopped before the message reached a worker
            await ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to dispatch message {TaskId}, returning it to the broker",
                message.Message.Id
            );
            await ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
    }

    private async Task ProcessMessagesAsync(
        int workerId,
        CancellationToken intakeToken,
        CancellationToken executionToken
    )
    {
        _logger.LogDebug("Worker thread {WorkerId} started", workerId);

        // Read until the dispatcher completes the channel so every dispatched message is settled
        await foreach (var message in _workChannel.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            // Register before checking for shutdown: a graceful shutdown that has already
            // started either waits for this task or sees the message returned to the broker.
            using var registration = _shutdownHandler?.RegisterTask(message.Message.Id, message);

            if (intakeToken.IsCancellationRequested)
            {
                // Prefetched but not started, so another worker can take it
                await ReturnToBrokerAsync(message).ConfigureAwait(false);
                continue;
            }

            await ProcessMessageAsync(message, executionToken).ConfigureAwait(false);
        }

        _logger.LogDebug("Worker thread {WorkerId} stopped", workerId);
    }

    private async Task ProcessMessageAsync(BrokerMessage message, CancellationToken executionToken)
    {
        TaskResult result;

        try
        {
            result = await _executor
                .ExecuteAsync(message, _workerName, executionToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (executionToken.IsCancellationRequested)
        {
            await HandleInterruptedAsync(message).ConfigureAwait(false);
            return;
        }
        catch (UnknownTaskException ex)
        {
            // Redelivering a message that no worker can handle would loop forever
            _killSwitch?.RecordFailure(ex);
            _logger.LogError(
                ex,
                "Rejecting message {TaskId} for unregistered task {TaskName}",
                message.Message.Id,
                message.Message.Task
            );
            await RejectAsync(message).ConfigureAwait(false);
            return;
        }
        catch (Exception ex)
        {
            // The outcome was not recorded (for example, the result backend is unavailable)
            _killSwitch?.RecordFailure(ex);
            _logger.LogError(
                ex,
                "Failed to process message {TaskId}, returning it to the broker",
                message.Message.Id
            );
            await ReturnToBrokerAfterFailureAsync(message, executionToken).ConfigureAwait(false);
            return;
        }

        // Record success/failure for kill switch
        if (result.State == TaskState.Success)
        {
            _killSwitch?.RecordSuccess();
        }
        else if (result.State == TaskState.Failure || result.State == TaskState.Rejected)
        {
            _killSwitch?.RecordFailure();
        }

        if (result.State == TaskState.Retry)
        {
            try
            {
                await ScheduleRetryAsync(message, result).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The retry was not scheduled, so keep the original message
                _logger.LogError(
                    ex,
                    "Failed to schedule retry for task {TaskId}, returning the message to the broker",
                    message.Message.Id
                );
                await ReturnToBrokerAfterFailureAsync(message, executionToken)
                    .ConfigureAwait(false);
                return;
            }

            await AckAsync(message).ConfigureAwait(false);
        }
        else if (result.State == TaskState.Requeued)
        {
            // Requeue the message for later processing (e.g., partition locked)
            // Apply requeue delay to prevent hot loops
            if (result.RequeueDelay.HasValue && result.RequeueDelay.Value > TimeSpan.Zero)
            {
                _logger.LogDebug(
                    "Task {TaskId} requeued with delay {RequeueDelay}",
                    message.Message.Id,
                    result.RequeueDelay.Value
                );
                await DelayAsync(result.RequeueDelay.Value, executionToken).ConfigureAwait(false);
            }
            else
            {
                _logger.LogDebug("Task {TaskId} requeued for later processing", message.Message.Id);
            }

            await ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
        else
        {
            // Success, Failure, Rejected, and Revoked outcomes are already stored
            await AckAsync(message).ConfigureAwait(false);
        }
    }

    private async Task ScheduleRetryAsync(BrokerMessage message, TaskResult result)
    {
        // Only increment retries if the task actually executed and failed
        // Rate-limited tasks never executed, so they shouldn't count toward max retries
        var retryCount = result.DoNotIncrementRetries
            ? message.Message.Retries
            : message.Message.Retries + 1;

        var retryMessage = message.Message with { Retries = retryCount };

        // Check if this is a rate-limited retry with a delay
        if (result.RetryAfter.HasValue && result.RetryAfter.Value > TimeSpan.Zero)
        {
            var deliveryTime = _timeProvider.GetUtcNow().Add(result.RetryAfter.Value);

            if (_options.RequeueRateLimitedToDelayQueue && _delayedMessageStore is not null)
            {
                // Add to delay store for delayed requeue
                await _delayedMessageStore
                    .AddAsync(retryMessage, deliveryTime, CancellationToken.None)
                    .ConfigureAwait(false);

                _logger.LogDebug(
                    "Task {TaskId} rate limited, scheduled for retry at {DeliveryTime}",
                    message.Message.Id,
                    deliveryTime
                );
            }
            else
            {
                // Set ETA on the message for the broker to handle
                retryMessage = retryMessage with
                {
                    Eta = deliveryTime,
                };
                await _broker
                    .PublishAsync(retryMessage, CancellationToken.None)
                    .ConfigureAwait(false);

                _logger.LogDebug(
                    "Task {TaskId} rate limited, requeued with ETA {Eta}",
                    message.Message.Id,
                    deliveryTime
                );
            }
        }
        else
        {
            // Regular retry - requeue immediately
            await _broker.PublishAsync(retryMessage, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task HandleInterruptedAsync(BrokerMessage message)
    {
        if (_options.NackOnForcedShutdown)
        {
            _logger.LogWarning(
                "Task {TaskId} did not finish before shutdown, returning it to the broker",
                message.Message.Id
            );
            await ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
        else
        {
            _logger.LogWarning(
                "Task {TaskId} did not finish before shutdown, leaving its message unacknowledged",
                message.Message.Id
            );
        }
    }

    private async Task ReturnToBrokerAfterFailureAsync(
        BrokerMessage message,
        CancellationToken executionToken
    )
    {
        await DelayAsync(_options.InfrastructureFailureRequeueDelay, executionToken)
            .ConfigureAwait(false);
        await ReturnToBrokerAsync(message).ConfigureAwait(false);
    }

    private async Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        try
        {
            await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Stopping: settle the message without waiting out the delay
        }
    }

    // Settlement uses CancellationToken.None so it completes during shutdown. A failed settle
    // leaves the message unsettled, and the broker redelivers it later.
    private Task AckAsync(BrokerMessage message) =>
        SettleAsync(
            message,
            "acknowledge",
            static (broker, m) => broker.AckAsync(m, CancellationToken.None)
        );

    private Task RejectAsync(BrokerMessage message) =>
        SettleAsync(
            message,
            "reject",
            static (broker, m) => broker.RejectAsync(m, requeue: false, CancellationToken.None)
        );

    private Task ReturnToBrokerAsync(BrokerMessage message) =>
        SettleAsync(
            message,
            "requeue",
            static (broker, m) => broker.RejectAsync(m, requeue: true, CancellationToken.None)
        );

    private async Task SettleAsync(
        BrokerMessage message,
        string action,
        Func<IMessageBroker, BrokerMessage, ValueTask> settle
    )
    {
        try
        {
            await settle(_broker, message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to {Action} message {TaskId}", action, message.Message.Id);
        }
    }

    private async Task CloseConsumerAsync(
        IAsyncEnumerator<BrokerMessage> consumer,
        CancellationTokenSource consumeCts,
        Task<bool>? pendingRead
    )
    {
        await consumeCts.CancelAsync().ConfigureAwait(false);

        if (pendingRead is not null)
        {
            try
            {
                if (await pendingRead.ConfigureAwait(false))
                {
                    // Delivered after intake stopped
                    await ReturnToBrokerAsync(consumer.Current).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected: the read was cancelled when the consumer closed
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Broker read failed while worker {WorkerName} was stopping",
                    _workerName
                );
            }
        }

        try
        {
            await consumer.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to close the broker consumer for worker {WorkerName}",
                _workerName
            );
        }
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Stop taking messages first. Prefetched messages are returned to the broker, and the
        // broker consumer stays open until every in-flight message is settled.
        await _stopIntakeCts.CancelAsync().ConfigureAwait(false);

        if (!_options.EnableGracefulShutdown || _shutdownHandler is null)
        {
            _logger.LogInformation(
                "Worker {WorkerName} stopping without graceful shutdown",
                _workerName
            );
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        _logger.LogInformation(
            "Worker {WorkerName} initiating graceful shutdown with timeout {Timeout}",
            _workerName,
            _options.ShutdownTimeout
        );

        GracefulShutdownResult? result = null;

        try
        {
            result = await _shutdownHandler
                .ShutdownAsync(_options.ShutdownTimeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host's shutdown budget ran out before the worker's own timeout
        }

        if (result is { CompletedGracefully: true })
        {
            _logger.LogInformation(
                "Worker {WorkerName} graceful shutdown completed. All {Total} tasks finished in {Duration}",
                _workerName,
                result.TotalTasks,
                result.Duration
            );
        }
        else
        {
            _logger.LogWarning(
                "Worker {WorkerName} forced shutdown. Completed: {Completed}, Cancelled: {Cancelled}",
                _workerName,
                result?.CompletedTasks ?? 0,
                result?.CancelledTasks ?? _shutdownHandler.ActiveTaskCount
            );

            // Cancel the remaining tasks. Each worker settles its own message once its task
            // has stopped, so a message is never requeued while its task is still running.
            await _abortExecutionCts.CancelAsync().ConfigureAwait(false);
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        base.Dispose();
        _stopIntakeCts.Dispose();
        _abortExecutionCts.Dispose();
    }
}
