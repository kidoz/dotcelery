using Microsoft.Extensions.Logging;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Helpers for the queue metrics a broker records as it publishes.
/// </summary>
public static class QueueMetricsExtensions
{
    /// <summary>
    /// Records that a message was enqueued, when a queue metrics store is registered.
    /// </summary>
    /// <remarks>
    /// A broker records the enqueue after the message is already in its queue, so a failure to
    /// record never fails the publish: the caller must not read a metrics failure as a failed
    /// send and publish the message again.
    /// </remarks>
    /// <param name="metrics">The queue metrics store, when one is registered.</param>
    /// <param name="queue">The queue the message was published to.</param>
    /// <param name="logger">The logger to report a failure to, when there is one.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async ValueTask RecordEnqueueAsync(
        this IQueueMetrics? metrics,
        string queue,
        ILogger? logger = null,
        CancellationToken cancellationToken = default
    )
    {
        if (metrics is null)
        {
            return;
        }

        try
        {
            await metrics.RecordEnqueuedAsync(queue, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The caller cancelled while the record was written; the message is published either
            // way, so the cancellation settles here instead of at the caller
        }
        catch (Exception ex)
        {
            logger?.LogDebug(
                ex,
                "Failed to record the enqueue of a message in queue {Queue}",
                queue
            );
        }
    }
}
