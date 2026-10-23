namespace DotCelery.Core.Batches;

/// <summary>
/// The task to run when a batch finishes, however it finished.
/// </summary>
/// <remarks>
/// The callback is dispatched by the worker that settles the last task of the batch, with the
/// input given when the batch was created and the batch ID on the message.
/// </remarks>
public sealed record BatchCallback
{
    /// <summary>
    /// Gets the name of the task to run.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the ID the task runs with, when the caller assigned one, such as a chord body.
    /// </summary>
    public string? TaskId { get; init; }

    /// <summary>
    /// Gets the serialized input the task receives.
    /// </summary>
#pragma warning disable CA1819 // Properties should not return arrays - Required for serialization
    public byte[]? Args { get; init; }
#pragma warning restore CA1819

    /// <summary>
    /// Gets the content type of <see cref="Args"/>, such as <c>application/json</c>.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets the queue to run the task on.
    /// </summary>
    public required string Queue { get; init; }

    /// <summary>
    /// Gets the priority of the task.
    /// </summary>
    public int? Priority { get; init; }

    /// <summary>
    /// Gets the maximum number of retries of the task.
    /// </summary>
    public int? MaxRetries { get; init; }

    /// <summary>
    /// Gets the headers of the task.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
