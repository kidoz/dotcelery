namespace DotCelery.Core.Exceptions;

/// <summary>
/// Exception thrown to permanently reject a task (no retry).
/// </summary>
public sealed class RejectException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RejectException"/> class.
    /// </summary>
    public RejectException()
        : base("Task was rejected") { }

    /// <summary>
    /// Initializes a new instance of the <see cref="RejectException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    public RejectException(string message)
        : base(message) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="RejectException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The inner exception.</param>
    public RejectException(string message, Exception? innerException)
        : base(message, innerException) { }

    /// <summary>
    /// Gets or sets whether to requeue the message.
    /// </summary>
    public bool Requeue { get; init; }
}
