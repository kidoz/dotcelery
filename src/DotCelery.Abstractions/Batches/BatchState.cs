namespace DotCelery.Core.Batches;

/// <summary>
/// Represents the state of a batch.
/// </summary>
public enum BatchState
{
    /// <summary>
    /// Batch is being created.
    /// </summary>
    Creating,

    /// <summary>
    /// Batch has been created and tasks are pending.
    /// </summary>
    Pending,

    /// <summary>
    /// Batch tasks are being processed.
    /// </summary>
    Processing,

    /// <summary>
    /// All batch tasks completed successfully.
    /// </summary>
    Completed,

    /// <summary>
    /// One or more batch tasks failed.
    /// </summary>
    Failed,

    /// <summary>
    /// Batch tasks completed with mixed results.
    /// </summary>
    PartiallyCompleted,

    /// <summary>
    /// Batch was cancelled.
    /// </summary>
    Cancelled,
}
