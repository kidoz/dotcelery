using DotCelery.Core.Abstractions;
using DotCelery.Core.Outbox;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Services;

/// <summary>
/// Background service that dispatches pending outbox messages to the broker.
/// </summary>
public sealed class OutboxDispatcher : BackgroundService
{
    private readonly IOutboxStore _outboxStore;
    private readonly IMessageBroker _broker;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxDispatcher> _logger;
    private readonly TimeProvider _timeProvider;

    private DateTimeOffset _lastCleanupAt;

    /// <summary>
    /// Initializes a new instance of the <see cref="OutboxDispatcher"/> class.
    /// </summary>
    public OutboxDispatcher(
        IOutboxStore outboxStore,
        IMessageBroker broker,
        IOptions<OutboxOptions> options,
        ILogger<OutboxDispatcher> logger,
        TimeProvider? timeProvider = null
    )
    {
        _outboxStore = outboxStore;
        _broker = broker;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _lastCleanupAt = _timeProvider.GetUtcNow();
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("Outbox dispatcher is disabled");
            return;
        }

        _logger.LogInformation(
            "Outbox dispatcher started. Dispatch interval: {Interval}, Batch size: {BatchSize}",
            _options.DispatchInterval,
            _options.BatchSize
        );

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await DispatchBatchAsync(stoppingToken).ConfigureAwait(false);
                await RunCleanupIfNeededAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error dispatching outbox messages");
            }

            try
            {
                await Task.Delay(_options.DispatchInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("Outbox dispatcher stopped");
    }

    private async Task DispatchBatchAsync(CancellationToken cancellationToken)
    {
        var dispatched = 0;
        var failed = 0;

        await foreach (
            var message in _outboxStore
                .GetPendingAsync(_options.BatchSize, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            try
            {
                await _broker
                    .PublishAsync(message.TaskMessage, cancellationToken)
                    .ConfigureAwait(false);

                await _outboxStore
                    .MarkDispatchedAsync(message.Id, cancellationToken)
                    .ConfigureAwait(false);

                dispatched++;

                _logger.LogDebug(
                    "Dispatched outbox message {MessageId} for task {TaskId}",
                    message.Id,
                    message.TaskMessage.Id
                );
            }
            catch (Exception ex)
            {
                failed++;

                await _outboxStore
                    .MarkFailedAsync(message.Id, ex.Message, cancellationToken)
                    .ConfigureAwait(false);

                _logger.LogWarning(
                    ex,
                    "Failed to dispatch outbox message {MessageId} (attempt {Attempt})",
                    message.Id,
                    message.Attempts + 1
                );
            }
        }

        if (dispatched > 0 || failed > 0)
        {
            _logger.LogDebug(
                "Outbox dispatch batch: {Dispatched} dispatched, {Failed} failed",
                dispatched,
                failed
            );
        }
    }

    private async Task RunCleanupIfNeededAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();

        if (now - _lastCleanupAt >= _options.CleanupInterval)
        {
            var cleaned = await _outboxStore
                .CleanupAsync(_options.RetentionPeriod, cancellationToken)
                .ConfigureAwait(false);

            if (cleaned > 0)
            {
                _logger.LogInformation("Cleaned up {Count} old outbox messages", cleaned);
            }

            _lastCleanupAt = now;
        }
    }
}
