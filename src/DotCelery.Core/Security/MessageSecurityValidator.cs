using System.Security.Cryptography;
using System.Text;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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
}

/// <summary>
/// Default implementation of <see cref="IMessageSecurityValidator"/>.
/// </summary>
public sealed class MessageSecurityValidator : IMessageSecurityValidator, IDisposable
{
    private readonly MessageSecurityOptions _options;
    private readonly ILogger<MessageSecurityValidator> _logger;
    private readonly HMACSHA256? _hmac;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageSecurityValidator"/> class.
    /// </summary>
    public MessageSecurityValidator(
        IOptions<MessageSecurityOptions> options,
        ILogger<MessageSecurityValidator> logger
    )
    {
        _options = options.Value;
        _logger = logger;

        if (_options.EnableMessageSigning && _options.SigningKey is not null)
        {
            _hmac = new HMACSHA256(_options.SigningKey);
        }
    }

    /// <inheritdoc />
    public MessageValidationResult Validate(TaskMessage message, string? signature = null)
    {
        ArgumentNullException.ThrowIfNull(message);

        // Check schema version
        if (message.SchemaVersion > _options.MaxAllowedSchemaVersion)
        {
            _logger.LogWarning(
                "Message {MessageId} has unsupported schema version {Version} (max: {MaxVersion})",
                message.Id,
                message.SchemaVersion,
                _options.MaxAllowedSchemaVersion
            );
            return MessageValidationResult.Failure(
                MessageValidationError.UnsupportedSchemaVersion,
                $"Schema version {message.SchemaVersion} is not supported. Maximum allowed: {_options.MaxAllowedSchemaVersion}"
            );
        }

        // Check payload size
        if (_options.MaxPayloadSizeBytes > 0 && message.Args.Length > _options.MaxPayloadSizeBytes)
        {
            _logger.LogWarning(
                "Message {MessageId} payload size {Size} exceeds limit {Limit}",
                message.Id,
                message.Args.Length,
                _options.MaxPayloadSizeBytes
            );
            return MessageValidationResult.Failure(
                MessageValidationError.PayloadTooLarge,
                $"Payload size {message.Args.Length} bytes exceeds maximum of {_options.MaxPayloadSizeBytes} bytes"
            );
        }

        // Check task allowlist
        if (_options.EnforceTaskAllowlist && !_options.AllowedTaskNames.Contains(message.Task))
        {
            _logger.LogWarning(
                "Message {MessageId} has disallowed task name {TaskName}",
                message.Id,
                message.Task
            );
            return MessageValidationResult.Failure(
                MessageValidationError.TaskNotAllowed,
                $"Task '{message.Task}' is not in the allowed task list"
            );
        }

        // Check signature if signing is enabled
        if (_options.EnableMessageSigning)
        {
            if (string.IsNullOrEmpty(signature))
            {
                if (_options.RejectUnsignedMessages)
                {
                    _logger.LogWarning(
                        "Message {MessageId} is missing required signature",
                        message.Id
                    );
                    return MessageValidationResult.Failure(
                        MessageValidationError.MissingSignature,
                        "Message signature is required but not provided"
                    );
                }

                _logger.LogDebug(
                    "Message {MessageId} has no signature (unsigned messages allowed)",
                    message.Id
                );
            }
            // Note: Actual signature verification is done in VerifySignature with the serialized payload
        }

        return MessageValidationResult.Success;
    }

    /// <inheritdoc />
    public bool ValidatePayloadSize(int payloadSize)
    {
        if (_options.MaxPayloadSizeBytes <= 0)
        {
            return true;
        }

        return payloadSize <= _options.MaxPayloadSizeBytes;
    }

    /// <inheritdoc />
    public string? Sign(byte[] payload)
    {
        if (_hmac is null)
        {
            return null;
        }

        var hash = _hmac.ComputeHash(payload);
        return Convert.ToBase64String(hash);
    }

    /// <inheritdoc />
    public bool VerifySignature(byte[] payload, string signature)
    {
        if (_hmac is null)
        {
            return false;
        }

        try
        {
            var expectedHash = _hmac.ComputeHash(payload);
            var actualHash = Convert.FromBase64String(signature);

            return CryptographicOperations.FixedTimeEquals(expectedHash, actualHash);
        }
        catch (FormatException)
        {
            _logger.LogWarning("Invalid signature format");
            return false;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _hmac?.Dispose();
    }
}
