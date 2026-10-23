namespace DotCelery.Core.Security;

/// <summary>
/// Exception thrown when a message fails security validation.
/// </summary>
public sealed class MessageSecurityException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageSecurityException"/> class.
    /// </summary>
    /// <param name="errorCode">The validation error code.</param>
    /// <param name="message">The error message.</param>
    public MessageSecurityException(MessageValidationError errorCode, string message)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageSecurityException"/> class.
    /// </summary>
    /// <param name="errorCode">The validation error code.</param>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public MessageSecurityException(
        MessageValidationError errorCode,
        string message,
        Exception innerException
    )
        : base(message, innerException)
    {
        ErrorCode = errorCode;
    }

    /// <summary>
    /// Gets the validation error code.
    /// </summary>
    public MessageValidationError ErrorCode { get; }

    /// <summary>
    /// Creates an exception from a validation result.
    /// </summary>
    /// <param name="result">The failed validation result.</param>
    /// <returns>A new exception.</returns>
    public static MessageSecurityException FromValidationResult(MessageValidationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        if (result.IsValid)
        {
            throw new ArgumentException(
                "Cannot create exception from successful validation",
                nameof(result)
            );
        }

        return new MessageSecurityException(
            result.ErrorCode ?? MessageValidationError.InvalidSignature,
            result.ErrorMessage ?? "Message validation failed"
        );
    }
}
