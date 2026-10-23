using DotCelery.Core.Signals;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Abstraction for queued signal storage and retrieval.
/// Enables asynchronous signal dispatch to a durable queue.
/// </summary>
public interface ISignalStore : IAsyncDisposable
{
    /// <summary>
    /// Enqueues a signal message for deferred processing.
    /// </summary>
    /// <param name="message">The signal message to enqueue.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask EnqueueAsync(SignalMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Dequeues signal messages for processing.
    /// </summary>
    /// <param name="batchSize">Maximum number of messages to dequeue.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async enumerable of signal messages.</returns>
    IAsyncEnumerable<SignalMessage> DequeueAsync(
        int batchSize = 100,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Acknowledges successful processing of a signal message.
    /// </summary>
    /// <param name="messageId">The message ID to acknowledge.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask AcknowledgeAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rejects a signal message, optionally returning it to the queue.
    /// </summary>
    /// <param name="messageId">The message ID to reject.</param>
    /// <param name="requeue">Whether to return the message to the queue.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask RejectAsync(
        string messageId,
        bool requeue = true,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the count of pending signals in the queue.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The count of pending signals.</returns>
    ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default);
}
