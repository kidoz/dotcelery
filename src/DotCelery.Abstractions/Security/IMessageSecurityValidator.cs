using DotCelery.Core.Models;

namespace DotCelery.Core.Security;

/// <summary>
/// Validates messages according to security policies.
/// </summary>
public interface IMessageSecurityValidator
{
    /// <summary>
    /// Validates a message before processing.
    /// </summary>
    /// <param name="message">The message to validate.</param>
    /// <param name="signature">The message signature (if signing is enabled).</param>
    /// <returns>A validation result indicating success or failure.</returns>
    MessageValidationResult Validate(TaskMessage message, string? signature = null);

    /// <summary>
    /// Validates a serialized message payload size.
    /// </summary>
    /// <param name="payloadSize">The size of the serialized payload in bytes.</param>
    /// <returns>True if the payload size is acceptable.</returns>
    bool ValidatePayloadSize(int payloadSize);

    /// <summary>
    /// Signs a message payload.
    /// </summary>
    /// <param name="payload">The serialized message payload.</param>
    /// <returns>The signature, or null if signing is disabled.</returns>
    string? Sign(byte[] payload);

    /// <summary>
    /// Verifies a message signature.
    /// </summary>
    /// <param name="payload">The serialized message payload.</param>
    /// <param name="signature">The signature to verify.</param>
    /// <returns>True if the signature is valid.</returns>
    bool VerifySignature(byte[] payload, string signature);
}

/// <summary>
/// Result of message validation.
/// </summary>
public sealed record MessageValidationResult
{
    /// <summary>
    /// Gets whether the validation passed.
    /// </summary>
    public required bool IsValid { get; init; }

    /// <summary>
    /// Gets the validation error message, if any.
    /// </summary>
    public string? ErrorMessage { get; init; }

    /// <summary>
    /// Gets the validation error code for programmatic handling.
    /// </summary>
    public MessageValidationError? ErrorCode { get; init; }

    /// <summary>
    /// Creates a successful validation result.
    /// </summary>
    public static MessageValidationResult Success { get; } = new() { IsValid = true };

    /// <summary>
    /// Creates a failed validation result.
    /// </summary>
    public static MessageValidationResult Failure(MessageValidationError code, string message) =>
        new()
        {
            IsValid = false,
            ErrorCode = code,
            ErrorMessage = message,
        };
}

/// <summary>
/// Validation error codes.
/// </summary>
public enum MessageValidationError
{
    /// <summary>Payload exceeds maximum size.</summary>
    PayloadTooLarge,

    /// <summary>Task name not in allowlist.</summary>
    TaskNotAllowed,

    /// <summary>Message signature is invalid.</summary>
    InvalidSignature,

    /// <summary>Message signature is missing.</summary>
    MissingSignature,

    /// <summary>Schema version not supported.</summary>
    UnsupportedSchemaVersion,

    /// <summary>Message is older than the configured maximum age.</summary>
    MessageExpired,
}
