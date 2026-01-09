namespace DotCelery.Cron;

/// <summary>
/// The exception that is thrown when a cron expression has an invalid format.
/// </summary>
public class CronFormatException : FormatException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CronFormatException"/> class.
    /// </summary>
    public CronFormatException()
        : base("The cron expression has an invalid format.") { }

    /// <summary>
    /// Initializes a new instance of the <see cref="CronFormatException"/> class
    /// with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public CronFormatException(string message)
        : base(message) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="CronFormatException"/> class
    /// with a specified error message and a reference to the inner exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public CronFormatException(string message, Exception innerException)
        : base(message, innerException) { }
}
