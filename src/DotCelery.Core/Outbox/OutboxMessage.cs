using DotCelery.Core.Models;

namespace DotCelery.Core.Outbox;

/// <summary>
/// Outbox message ready for dispatch.
/// </summary>
public sealed record OutboxMessage
{
    /// <summary>
    /// Gets the unique message identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the task message to be published.
    /// </summary>
    public required TaskMessage TaskMessage { get; init; }

    /// <summary>
    /// Gets when the message was stored.
    /// </summary>
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    /// Gets the current dispatch status.
    /// </summary>
    public OutboxMessageStatus Status { get; init; } = OutboxMessageStatus.Pending;

    /// <summary>
    /// Gets the number of dispatch attempts.
    /// </summary>
    public int Attempts { get; init; }

    /// <summary>
    /// Gets the last error message (if failed).
    /// </summary>
    public string? LastError { get; init; }

    /// <summary>
    /// Gets when the message was dispatched (if completed).
    /// </summary>
    public DateTimeOffset? DispatchedAt { get; init; }

    /// <summary>
    /// Gets the sequence number for ordering.
    /// </summary>
    public long SequenceNumber { get; init; }
}
