using DotCelery.Core.DeadLetter;
using DotCelery.Core.Models;

namespace DotCelery.Core.Abstractions;

/// <summary>
/// Handler for dead letter queue operations.
/// </summary>
public interface IDeadLetterHandler
{
    /// <summary>
    /// Handles a failed task by potentially adding it to the dead letter queue.
    /// </summary>
    /// <param name="message">The original task message.</param>
    /// <param name="reason">The reason for failure.</param>
    /// <param name="exception">The exception (if any).</param>
    /// <param name="worker">The worker that processed the task.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask HandleAsync(
        TaskMessage message,
        DeadLetterReason reason,
        Exception? exception = null,
        string? worker = null,
        CancellationToken cancellationToken = default
    );
}
