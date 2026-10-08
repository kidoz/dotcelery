namespace DotCelery.Core.Canvas;

/// <summary>
/// The input an error callback (<see cref="Signature.LinkError"/>) runs with: the task that
/// failed and the error it failed with.
/// </summary>
/// <remarks>
/// The worker serializes this payload into the callback message's arguments, so an error
/// callback task declares it as its input, for example <c>ITask&lt;TaskErrorInfo&gt;</c>.
/// </remarks>
public sealed record TaskErrorInfo
{
    /// <summary>
    /// Gets the ID of the task that failed.
    /// </summary>
    public required string TaskId { get; init; }

    /// <summary>
    /// Gets the name of the task that failed.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets the CLR type name of the exception the task failed with, if any.
    /// </summary>
    public string? ErrorType { get; init; }

    /// <summary>
    /// Gets the message of the exception the task failed with, if any.
    /// </summary>
    public string? ErrorMessage { get; init; }
}
