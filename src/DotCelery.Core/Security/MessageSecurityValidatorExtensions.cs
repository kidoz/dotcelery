using DotCelery.Core.Models;

namespace DotCelery.Core.Security;

/// <summary>
/// Helpers for verifying the signature of a serialized message payload, shared by the brokers
/// that check a payload before deserializing it and the worker that checks a delivered message.
/// </summary>
public static class MessageSecurityValidatorExtensions
{
    /// <summary>
    /// Checks a serialized payload against its signature.
    /// </summary>
    /// <remarks>
    /// A validator that requires signed messages rejects an unsigned payload here; an unsigned
    /// payload passes only when the validator allows unsigned messages. The message-level checks
    /// run against a placeholder because the payload is not deserialized yet, so only the
    /// signature outcome of the validation is used.
    /// </remarks>
    /// <param name="validator">The security validator, when one is registered.</param>
    /// <param name="payload">The serialized message payload.</param>
    /// <param name="signature">The signature carried with the payload, if any.</param>
    /// <returns>True when there is no validator or the payload's signature is acceptable.</returns>
    public static bool IsSignatureValid(
        this IMessageSecurityValidator? validator,
        byte[] payload,
        string? signature
    )
    {
        if (validator is null)
        {
            return true;
        }

        var validation = validator.Validate(
            new TaskMessage
            {
                Id = "unknown",
                Task = "unknown",
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            signature
        );

        return validator.IsSignatureValid(validation, payload, signature);
    }

    /// <summary>
    /// Checks a serialized payload against its signature after
    /// <see cref="IMessageSecurityValidator.Validate" /> has run on the message.
    /// </summary>
    /// <param name="validator">The security validator.</param>
    /// <param name="validation">The result of validating the message the payload holds.</param>
    /// <param name="payload">The serialized message payload.</param>
    /// <param name="signature">The signature carried with the payload, if any.</param>
    /// <returns>True when the payload's signature is acceptable.</returns>
    public static bool IsSignatureValid(
        this IMessageSecurityValidator validator,
        MessageValidationResult validation,
        byte[] payload,
        string? signature
    )
    {
        if (validation.ErrorCode == MessageValidationError.MissingSignature)
        {
            return false;
        }

        // An unsigned payload the validator accepted needs no verification; a signed one must
        // match its payload
        return string.IsNullOrEmpty(signature) || validator.VerifySignature(payload, signature);
    }
}
