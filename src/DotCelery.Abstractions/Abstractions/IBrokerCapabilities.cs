namespace DotCelery.Core.Abstractions;

/// <summary>
/// What a broker guarantees beyond the delivery contract every broker honors: publishing a
/// message to its queue, delivering it to one consumer, settling it with an acknowledgement or
/// a rejection, returning an unsettled message for redelivery, and preserving every field of
/// the message it was given.
/// </summary>
public sealed record MessageBrokerCapabilities
{
    /// <summary>
    /// Gets whether the broker delivers higher-priority messages first. RabbitMQ does when its
    /// queue declares a priority depth; the other bundled brokers deliver in publish order.
    /// </summary>
    public bool Priority { get; init; }

    /// <summary>
    /// Gets whether the broker holds a message until its ETA itself, so the worker does not wait
    /// on it. Amazon SQS does; without a delay store the other bundled brokers deliver the
    /// message and the worker waits on the ETA.
    /// </summary>
    public bool NativeDelay { get; init; }

    /// <summary>
    /// Gets whether a message whose consumer stopped without settling it is delivered again:
    /// RabbitMQ does when the consuming channel closes, and Redis, Amazon SQS, and the in-memory
    /// broker when the consumer stops or its claim expires.
    /// </summary>
    public bool RedeliveryOnConsumerStop { get; init; }

    /// <summary>
    /// Gets whether a requeued message with an ETA in the future is held until that ETA instead
    /// of being delivered again at once. Amazon SQS does; the other bundled brokers return it
    /// promptly and the worker waits on the ETA.
    /// </summary>
    public bool HoldsRequeuedMessagesUntilEta { get; init; }

    /// <summary>
    /// Gets whether the broker refuses to settle a delivery its consumer no longer owns, such as
    /// one that was settled already or whose consumer stopped. A refused settlement leaves the
    /// message for redelivery instead of settling a delivery that was handed to another
    /// consumer.
    /// </summary>
    public bool RejectsStaleSettlements { get; init; }

    /// <summary>
    /// Gets the largest message the broker accepts, or <c>null</c> when it does not enforce a
    /// limit.
    /// </summary>
    public int? MaxMessageSizeBytes { get; init; }
}

/// <summary>
/// Implemented by brokers that declare what they guarantee, so a caller can check whether a
/// broker meets its requirements instead of discovering the difference in production.
/// </summary>
public interface IBrokerCapabilities
{
    /// <summary>
    /// Gets the capabilities of this broker.
    /// </summary>
    MessageBrokerCapabilities Capabilities { get; }
}
