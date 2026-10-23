using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Republishes a task that asked to be retried, either immediately, after a rate-limit delay,
/// or in the delay store.
/// </summary>
internal sealed class RetryScheduler(
    IMessageBroker broker,
    IDelayedMessageStore? delayedMessageStore,
    WorkerOptions options,
    TimeProvider timeProvider,
    ILogger logger
)
{
    public async Task ScheduleAsync(BrokerMessage message, TaskResult result)
    {
        // Only increment retries if the task actually executed and failed. Rate-limited tasks
        // never executed, so they don't count toward max retries.
        var retryCount = result.DoNotIncrementRetries
            ? message.Message.Retries
            : message.Message.Retries + 1;

        var retryMessage = message.Message with { Retries = retryCount };

        if (result.RetryAfter.HasValue && result.RetryAfter.Value > TimeSpan.Zero)
        {
            var deliveryTime = timeProvider.GetUtcNow().Add(result.RetryAfter.Value);

            if (options.RequeueRateLimitedToDelayQueue && delayedMessageStore is not null)
            {
                await delayedMessageStore
                    .AddAsync(retryMessage, deliveryTime, CancellationToken.None)
                    .ConfigureAwait(false);

                logger.LogDebug(
                    "Task {TaskId} rate limited, scheduled for retry at {DeliveryTime}",
                    message.Message.Id,
                    deliveryTime
                );
            }
            else
            {
                retryMessage = retryMessage with { Eta = deliveryTime };
                await broker
                    .PublishAsync(retryMessage, CancellationToken.None)
                    .ConfigureAwait(false);

                logger.LogDebug(
                    "Task {TaskId} rate limited, requeued with ETA {Eta}",
                    message.Message.Id,
                    deliveryTime
                );
            }
        }
        else
        {
            await broker.PublishAsync(retryMessage, CancellationToken.None).ConfigureAwait(false);
        }
    }
}
