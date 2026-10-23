using System.Security.Cryptography;
using System.Text.Json;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Security;

/// <summary>
/// Default implementation of <see cref="IMessageSecurityValidator"/>.
/// </summary>
public sealed class MessageSecurityValidator : IMessageSecurityValidator, IDisposable
{
    private readonly MessageSecurityOptions _options;
    private readonly ILogger<MessageSecurityValidator> _logger;
    private readonly TimeProvider _timeProvider;

    // HMACSHA256 instances are not thread-safe, and this validator is a shared singleton,
    // so each signature is computed with the stateless HMACSHA256.HashData API.
    private readonly byte[]? _signingKey;
    private readonly Dictionary<string, byte[]> _tenantKeys = new(StringComparer.Ordinal);
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="MessageSecurityValidator"/> class.
    /// </summary>
    /// <param name="options">The security options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">The clock for the maximum message age.</param>
    public MessageSecurityValidator(
        IOptions<MessageSecurityOptions> options,
        ILogger<MessageSecurityValidator> logger,
        TimeProvider? timeProvider = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;

        if (_options.EnableMessageSigning)
        {
            foreach (var (tenant, key) in _options.TenantSigningKeys)
                _tenantKeys.Add(tenant, (byte[])key.Clone());
        }

        if (_options.EnableMessageSigning && _options.SigningKey is not null)
        {
            _signingKey = (byte[])_options.SigningKey.Clone();
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

        // Check the message age, which narrows the window in which a captured signed message
        // can be replayed
        if (_options.MaxMessageAge is { } maxAge)
        {
            var age = _timeProvider.GetUtcNow() - message.Timestamp;
            if (age > maxAge)
            {
                _logger.LogWarning(
                    "Message {MessageId} is {Age} old, which exceeds the maximum of {MaxAge}",
                    message.Id,
                    age,
                    maxAge
                );
                return MessageValidationResult.Failure(
                    MessageValidationError.MessageExpired,
                    $"Message is {age} old, which exceeds the maximum age of {maxAge}"
                );
            }
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
        ObjectDisposedException.ThrowIf(_disposed, this);
        var key = ResolveKey(payload);
        if (key is null)
        {
            if (_tenantKeys.Count > 0)
                throw new InvalidOperationException(
                    "No signing key is available for the message's explicit tenant ID."
                );
            return null;
        }
        return Convert.ToBase64String(HMACSHA256.HashData(key, payload));
    }

    private byte[]? ResolveKey(byte[] payload)
    {
        if (_tenantKeys.Count == 0)
            return _signingKey;
        try
        {
            var message = JsonSerializer.Deserialize(
                payload,
                DotCeleryJsonContext.Default.TaskMessage
            );
            return message?.TenantId is { } tenant && _tenantKeys.TryGetValue(tenant, out var key)
                ? key
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public bool VerifySignature(byte[] payload, string signature)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var key = ResolveKey(payload);
        if (key is null)
            return false;

        try
        {
            var expectedHash = HMACSHA256.HashData(key, payload);
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

        foreach (var key in _tenantKeys.Values)
            CryptographicOperations.ZeroMemory(key);

        if (_signingKey is not null)
        {
            CryptographicOperations.ZeroMemory(_signingKey);
        }
    }
}
