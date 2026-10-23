namespace DotCelery.Core.Canvas;

/// <summary>
/// A step of a chain that a task continues with when it succeeds.
/// </summary>
/// <remarks>
/// The step travels with the previous task's message, so the worker that runs a step publishes
/// the next one with its result; no scheduler or stored workflow is needed.
/// </remarks>
public sealed record ChainStep
{
    /// <summary>
    /// Gets the ID the next task runs with, assigned when the chain was submitted.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the signature of the next task.
    /// </summary>
    public required Signature Signature { get; init; }
}
