namespace DotCelery.Core.Outbox;

/// <summary>
/// Status of an outbox message.
/// </summary>
public enum OutboxMessageStatus
{
    /// <summary>
    /// Message is pending dispatch.
    /// </summary>
    Pending,

    /// <summary>
    /// Message has been dispatched to the broker.
    /// </summary>
    Dispatched,

    /// <summary>
    /// Message dispatch failed after max retries.
    /// </summary>
    Failed,
}
