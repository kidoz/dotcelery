using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Worker.Execution;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker;

/// <summary>
/// Background service that runs the Celery worker.
/// </summary>
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

        // Start worker tasks
        var workers = Enumerable
            .Range(0, _options.Concurrency)
            .Select(i => ProcessMessagesAsync(i, stoppingToken))
            .ToList();

        // Start consumer
        var consumer = ConsumeMessagesAsync(stoppingToken);

        try
        {
            await Task.WhenAll([consumer, .. workers]).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Worker {WorkerName} shutting down gracefully", _workerName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Worker {WorkerName} encountered an error", _workerName);
            throw;
        }
    }

    private async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (
                var message in _broker
                    .ConsumeAsync(_options.Queues.ToList(), cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                // Wait if kill switch is tripped
                if (_killSwitch is not null)
                {
                    await _killSwitch.WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
                }

                var now = _timeProvider.GetUtcNow();

                // Check if task has expired
                if (message.Message.Expires.HasValue && message.Message.Expires.Value < now)
                {
                    _logger.LogWarning("Task {TaskId} has expired, skipping", message.Message.Id);
                    await _broker.AckAsync(message, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                // Check if ETA is in the future
                if (message.Message.Eta.HasValue && message.Message.Eta.Value > now)
                {
                    if (_options.UseDelayQueue && _delayedMessageStore is not null)
                    {
                        // Add to delay store for efficient handling
                        await _delayedMessageStore
                            .AddAsync(message.Message, message.Message.Eta.Value, cancellationToken)
                            .ConfigureAwait(false);

                        await _broker.AckAsync(message, cancellationToken).ConfigureAwait(false);

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

                        await Task.Delay(actualDelay, cancellationToken).ConfigureAwait(false);
                        await _broker
                            .RejectAsync(message, requeue: true, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    continue;
                }

                await _workChannel
                    .Writer.WriteAsync(message, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            _workChannel.Writer.Complete();
        }
    }

    private async Task ProcessMessagesAsync(int workerId, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Worker thread {WorkerId} started", workerId);

        await foreach (
            var message in _workChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)
        )
        {
            // Register task for graceful shutdown tracking
            using var registration = _shutdownHandler?.RegisterTask(message.Message.Id, message);

            try
            {
                var result = await _executor
                    .ExecuteAsync(message, _workerName, cancellationToken)
                    .ConfigureAwait(false);

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
                    await HandleRetryAsync(message, result, cancellationToken)
                        .ConfigureAwait(false);
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
                        await Task.Delay(result.RequeueDelay.Value, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        _logger.LogDebug(
                            "Task {TaskId} requeued for later processing",
                            message.Message.Id
                        );
                    }

                    await _broker
                        .RejectAsync(message, requeue: true, cancellationToken)
                        .ConfigureAwait(false);
                }
                else if (result.State == TaskState.Revoked)
                {
                    // Just ack the message, result is already stored
                    await _broker.AckAsync(message, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await _broker.AckAsync(message, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _killSwitch?.RecordFailure(ex);
                _logger.LogError(ex, "Failed to process message {TaskId}", message.Message.Id);
                await _broker
                    .RejectAsync(message, requeue: false, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        _logger.LogDebug("Worker thread {WorkerId} stopped", workerId);
    }

    private async Task HandleRetryAsync(
        BrokerMessage message,
        TaskResult result,
        CancellationToken cancellationToken
    )
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
                    .AddAsync(retryMessage, deliveryTime, cancellationToken)
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
                await _broker.PublishAsync(retryMessage, cancellationToken).ConfigureAwait(false);

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
            await _broker.PublishAsync(retryMessage, cancellationToken).ConfigureAwait(false);
        }

        await _broker.AckAsync(message, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
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

        var result = await _shutdownHandler
            .ShutdownAsync(_options.ShutdownTimeout, cancellationToken)
            .ConfigureAwait(false);

        if (result.CompletedGracefully)
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
                result.CompletedTasks,
                result.CancelledTasks
            );

            if (_options.NackOnForcedShutdown)
            {
                var pendingMessages = _shutdownHandler.GetPendingMessages();

                foreach (var message in pendingMessages)
                {
                    try
                    {
                        await _broker
                            .RejectAsync(message, requeue: true, CancellationToken.None)
                            .ConfigureAwait(false);

                        _logger.LogDebug(
                            "Task {TaskId} nacked for redelivery during forced shutdown",
                            message.Message.Id
                        );
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(
                            ex,
                            "Failed to nack task {TaskId} during forced shutdown",
                            message.Message.Id
                        );
                    }
                }

                _logger.LogInformation(
                    "Nacked {Count} messages for redelivery",
                    pendingMessages.Count
                );
            }
        }

        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}
