namespace DotCelery.Core.Exceptions;

/// <summary>
/// Exception thrown to request a task retry.
/// </summary>
public sealed class RetryException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RetryException"/> class.
    /// </summary>
    public RetryException()
        : base("Task requested retry") { }

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    public RetryException(string message)
        : base(message) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryException"/> class.
    /// </summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The inner exception.</param>
    public RetryException(string message, Exception? innerException)
        : base(message, innerException) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryException"/> class.
    /// </summary>
    /// <param name="countdown">Delay before retry.</param>
    /// <param name="innerException">The original exception.</param>
    public RetryException(TimeSpan? countdown, Exception? innerException)
        : base("Task requested retry", innerException)
    {
        Countdown = countdown;
    }

    /// <summary>
    /// Gets the delay before retry.
    /// </summary>
    public TimeSpan? Countdown { get; }
}
