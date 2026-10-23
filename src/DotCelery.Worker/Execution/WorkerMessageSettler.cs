using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Settles a worker's broker deliveries: acknowledges, rejects, or returns a message to its
/// broker, and stores a rejected message in the dead letter queue first.
/// </summary>
/// <remarks>
/// Settlement uses <see cref="CancellationToken.None"/> so it completes during shutdown. A
/// failed settlement leaves the message unsettled, and the broker redelivers it later.
/// </remarks>
internal sealed class WorkerMessageSettler(
    IMessageBroker broker,
    IDeadLetterHandler? deadLetterHandler,
    ILogger logger,
    string workerName,
    TimeProvider timeProvider
)
{
    /// <summary>
    /// Stores a message in the dead letter queue when one is configured, and then reports
    /// whether the caller may settle it.
    /// </summary>
    /// <returns>
    /// <c>true</c> when the message is handled and may be settled; <c>false</c> when the dead
    /// letter queue could not store it, in which case the message was returned to the broker.
    /// </returns>
    public async Task<bool> DeadLetterAsync(BrokerMessage delivery, DeadLetterReason reason)
    {
        if (deadLetterHandler is null)
        {
            return true;
        }

        var message = delivery.Message;

        try
        {
            await deadLetterHandler
                .HandleAsync(
                    message,
                    reason,
                    worker: workerName,
                    cancellationToken: CancellationToken.None
                )
                .ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Failed to store task {TaskId} in the dead letter queue",
                message.Id
            );
            await ReturnToBrokerAsync(delivery).ConfigureAwait(false);
            return false;
        }
    }

    public Task AckAsync(BrokerMessage message) =>
        SettleAsync(
            message,
            "acknowledge",
            static (target, delivery) => target.AckAsync(delivery, CancellationToken.None)
        );

    public Task RejectAsync(BrokerMessage message) =>
        SettleAsync(
            message,
            "reject",
            static (target, delivery) =>
                target.RejectAsync(delivery, requeue: false, CancellationToken.None)
        );

    public Task ReturnToBrokerAsync(BrokerMessage message) =>
        SettleAsync(
            message,
            "requeue",
            static (target, delivery) =>
                target.RejectAsync(delivery, requeue: true, CancellationToken.None)
        );

    /// <summary>
    /// Waits out an infrastructure failure delay, then returns the message to its broker.
    /// </summary>
    public async Task ReturnAfterFailureAsync(
        BrokerMessage message,
        TimeSpan delay,
        CancellationToken cancellationToken
    )
    {
        await DelayAsync(delay, cancellationToken).ConfigureAwait(false);
        await ReturnToBrokerAsync(message).ConfigureAwait(false);
    }

    /// <summary>
    /// Waits for a delay without failing when the wait is cancelled during shutdown.
    /// </summary>
    public async Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        if (delay <= TimeSpan.Zero)
        {
            return;
        }

        try
        {
            await Task.Delay(delay, timeProvider, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Stopping: settle the message without waiting out the delay
        }
    }

    private async Task SettleAsync(
        BrokerMessage message,
        string action,
        Func<IMessageBroker, BrokerMessage, ValueTask> settle
    )
    {
        try
        {
            await settle(broker, message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to {Action} message {TaskId}", action, message.Message.Id);
        }
    }
}
