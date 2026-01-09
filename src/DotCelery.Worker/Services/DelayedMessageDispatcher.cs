using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Services;

/// <summary>
/// Background service that dispatches delayed messages when they become due.
/// Polls the delayed message store and publishes due messages to the broker.
/// </summary>
public sealed class DelayedMessageDispatcher : BackgroundService
{
    private readonly IDelayedMessageStore _delayedMessageStore;
    private readonly IMessageBroker _broker;
    private readonly WorkerOptions _options;
    private readonly ILogger<DelayedMessageDispatcher> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="DelayedMessageDispatcher"/> class.
    /// </summary>
    public DelayedMessageDispatcher(
        IDelayedMessageStore delayedMessageStore,
        IMessageBroker broker,
        IOptions<WorkerOptions> options,
        ILogger<DelayedMessageDispatcher> logger,
        TimeProvider? timeProvider = null
    )
    {
        _delayedMessageStore = delayedMessageStore;
        _broker = broker;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.UseDelayQueue)
        {
            _logger.LogInformation("Delayed message dispatcher is disabled");
            return;
        }

        _logger.LogInformation(
            "Starting delayed message dispatcher with poll interval {PollInterval}",
            _options.DelayedMessagePollInterval
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DispatchDueMessagesAsync(stoppingToken).ConfigureAwait(false);

                // Calculate optimal wait time
                var waitTime = await CalculateWaitTimeAsync(stoppingToken).ConfigureAwait(false);

                if (waitTime > TimeSpan.Zero)
                {
                    await Task.Delay(waitTime, _timeProvider, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Expected during shutdown
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in delayed message dispatcher");

                // Wait before retrying on error
                await Task.Delay(_options.DelayedMessagePollInterval, _timeProvider, stoppingToken)
                    .ConfigureAwait(false);
            }
        }

        _logger.LogInformation("Delayed message dispatcher stopped");
    }

    private async Task DispatchDueMessagesAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var dispatchedCount = 0;

        await foreach (
            var message in _delayedMessageStore
                .GetDueMessagesAsync(now, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            try
            {
                // Clear the ETA since we're dispatching now
                var dispatchMessage = message with
                {
                    Eta = null,
                };

                await _broker
                    .PublishAsync(dispatchMessage, cancellationToken)
                    .ConfigureAwait(false);

                _logger.LogDebug(
                    "Dispatched delayed message {TaskId} to queue {Queue}",
                    message.Id,
                    message.Queue
                );

                dispatchedCount++;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to dispatch delayed message {TaskId}", message.Id);

                // Re-add the message to the store for retry
                try
                {
                    var retryTime = now.Add(_options.DelayedMessageRetryInterval);
                    await _delayedMessageStore
                        .AddAsync(message, retryTime, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception retryEx)
                {
                    _logger.LogError(
                        retryEx,
                        "Failed to re-add message {TaskId} to delay store",
                        message.Id
                    );
                }
            }
        }

        if (dispatchedCount > 0)
        {
            _logger.LogDebug("Dispatched {Count} delayed messages", dispatchedCount);
        }
    }

    private async Task<TimeSpan> CalculateWaitTimeAsync(CancellationToken cancellationToken)
    {
        var nextDelivery = await _delayedMessageStore
            .GetNextDeliveryTimeAsync(cancellationToken)
            .ConfigureAwait(false);

        if (!nextDelivery.HasValue)
        {
            // No messages pending, use default poll interval
            return _options.DelayedMessagePollInterval;
        }

        var now = _timeProvider.GetUtcNow();
        var timeUntilNext = nextDelivery.Value - now;

        if (timeUntilNext <= TimeSpan.Zero)
        {
            // Messages are already due
            return TimeSpan.Zero;
        }

        // Wait until next message is due, but cap at max poll interval
        return timeUntilNext < _options.DelayedMessagePollInterval
            ? timeUntilNext
            : _options.DelayedMessagePollInterval;
    }
}
