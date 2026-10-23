using DotCelery.Core.Abstractions;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Waits for a message's ETA in the worker, in one task slot, instead of holding the intake
/// loop.
/// </summary>
/// <remarks>
/// The wait is capped at <see cref="WorkerOptions.MaxEtaWait"/>; a message whose ETA is further
/// out goes back to the broker to be redelivered, and one that expires while waiting is
/// dead-lettered.
/// </remarks>
internal sealed class EtaWaiter(
    WorkerOptions options,
    TimeProvider timeProvider,
    ILogger logger,
    WorkerMessageSettler settler
)
{
    /// <summary>
    /// Waits until the message's ETA, if it is still ahead.
    /// </summary>
    /// <returns><c>true</c> when the message should be processed now.</returns>
    public async Task<bool> WaitAsync(BrokerMessage message, CancellationToken executionToken)
    {
        if (message.Message.Eta is not { } eta || eta <= timeProvider.GetUtcNow())
        {
            return true;
        }

        var wait = eta - timeProvider.GetUtcNow();
        if (wait > options.MaxEtaWait)
        {
            wait = options.MaxEtaWait;
        }

        await settler.DelayAsync(wait, executionToken).ConfigureAwait(false);

        if (eta > timeProvider.GetUtcNow())
        {
            logger.LogDebug(
                "Task {TaskId} has future ETA {Eta}, returning it to the broker to be redelivered",
                message.Message.Id,
                eta
            );
            await settler.ReturnToBrokerAsync(message).ConfigureAwait(false);
            return false;
        }

        // The wait can carry a message past its expiry, which the intake loop checked before it
        // handed the message over
        if (
            message.Message.Expires.HasValue
            && message.Message.Expires.Value < timeProvider.GetUtcNow()
        )
        {
            logger.LogWarning("Task {TaskId} has expired, skipping", message.Message.Id);
            if (
                !await settler
                    .DeadLetterAsync(message, DeadLetterReason.Expired)
                    .ConfigureAwait(false)
            )
            {
                return false;
            }
            await settler.AckAsync(message).ConfigureAwait(false);
            return false;
        }

        return true;
    }
}
