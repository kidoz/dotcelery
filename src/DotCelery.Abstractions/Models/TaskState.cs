namespace DotCelery.Core.Models;

/// <summary>
/// Task execution states.
/// </summary>
public enum TaskState
{
    /// <summary>
    /// Task is waiting to be executed.
    /// </summary>
    Pending,

    /// <summary>
    /// Task has been received by a worker.
    /// </summary>
    Received,

    /// <summary>
    /// Task execution started.
    /// </summary>
    Started,

    /// <summary>
    /// Task completed successfully.
    /// </summary>
    Success,

    /// <summary>
    /// Task failed with exception.
    /// </summary>
    Failure,

    /// <summary>
    /// Task was revoked/cancelled.
    /// </summary>
    Revoked,

    /// <summary>
    /// Task was rejected (won't retry).
    /// </summary>
    Rejected,

    /// <summary>
    /// Task is retrying.
    /// </summary>
    Retry,

    /// <summary>
    /// Custom progress update.
    /// </summary>
    Progress,

    /// <summary>
    /// Task was requeued for later processing (e.g., partition locked).
    /// </summary>
    Requeued,
}
