using DotCelery.Core.Models;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Message broker abstraction for task transport.
/// </summary>
public interface IMessageBroker : IAsyncDisposable
{
    /// <summary>
    /// Publishes a task message to the specified queue.
    /// </summary>
    /// <param name="message">The task message to publish.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A value task representing the async operation.</returns>
    ValueTask PublishAsync(TaskMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Consumes messages from the specified queues.
    /// </summary>
    /// <param name="queues">Queues to consume from.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Async stream of consumed messages.</returns>
    IAsyncEnumerable<BrokerMessage> ConsumeAsync(
        IReadOnlyList<string> queues,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Acknowledges successful message processing.
    /// </summary>
    /// <param name="message">The message to acknowledge.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A value task representing the async operation.</returns>
    ValueTask AckAsync(BrokerMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rejects a message, optionally requeueing it.
    /// </summary>
    /// <param name="message">The message to reject.</param>
    /// <param name="requeue">Whether to requeue the message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A value task representing the async operation.</returns>
    ValueTask RejectAsync(
        BrokerMessage message,
        bool requeue = false,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Checks broker connection health.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>True if the broker is healthy.</returns>
    ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Message received from broker with delivery context.
/// </summary>
public sealed class BrokerMessage
{
    /// <summary>
    /// Gets the task message payload.
    /// </summary>
    public required TaskMessage Message { get; init; }

    /// <summary>
    /// Gets the broker-specific delivery tag for acknowledgment.
    /// </summary>
    /// <remarks>
    /// The type varies by broker implementation (e.g., <c>ulong</c> for RabbitMQ, <c>Guid</c> for InMemory).
    /// Broker implementations should use pattern matching to extract the expected type:
    /// <code>if (message.DeliveryTag is not ulong deliveryTag) throw ...</code>
    /// </remarks>
    public required object DeliveryTag { get; init; }

    /// <summary>
    /// Gets the queue the message was received from.
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// Gets when the message was received.
    /// </summary>
    public required DateTimeOffset ReceivedAt { get; init; }
}
