using System.Text.Json;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Security;

/// <summary>
/// Default implementation of <see cref="IMessageSecurityValidator"/>.
/// </summary>
public sealed partial class MessageSecurityValidator : IMessageSecurityValidator, IDisposable
{
    private readonly MessageSecurityOptions _options;
    private readonly ILogger<MessageSecurityValidator> _logger;
    private readonly TimeProvider _timeProvider;

    private readonly MessageSigningKeys? _keys;
    private readonly Dictionary<string, MessageSigningKeys> _tenantKeys = new(
        StringComparer.Ordinal
    );
    private readonly string? _legacyKeyId;
    private readonly bool _sign;
    private readonly bool _verify;
    private int _reportedUnverifiedSignature;
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

        _sign = _options.EnableMessageSigning;
        _verify = _sign || _options.VerifySignaturesWhenPresent;
        _legacyKeyId = _options.LegacyVerificationKeyId;
        if (!_verify)
            return;

        foreach (
            var tenant in _options.TenantSigningKeys.Keys.Union(
                _options.TenantAdditionalVerificationKeys.Keys,
                StringComparer.Ordinal
            )
        )
            _tenantKeys.Add(
                tenant,
                new MessageSigningKeys(
                    _options.TenantSigningKeys.GetValueOrDefault(tenant),
                    _options.TenantSigningKeyIds.GetValueOrDefault(tenant),
                    _options.TenantAdditionalVerificationKeys.GetValueOrDefault(tenant)
                )
            );
        if (_tenantKeys.Count == 0)
            _keys = new MessageSigningKeys(
                _options.SigningKey,
                _options.SigningKeyId,
                _options.AdditionalVerificationKeys
            );
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
            if (age < -_options.MaxClockSkew)
                return MessageValidationResult.Failure(
                    MessageValidationError.MessageExpired,
                    "Message timestamp exceeds the allowed future clock skew."
                );
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
        if (!_sign)
            return null;
        var signature = ResolveKeys(payload)?.Sign(payload);
        if (signature is null && _tenantKeys.Count > 0)
            throw new InvalidOperationException(
                "No signing key is available for the message's explicit tenant ID."
            );
        return signature;
    }

    private MessageSigningKeys? ResolveKeys(byte[] payload)
    {
        if (_tenantKeys.Count == 0)
            return _keys;
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
        return ResolveKeys(payload)?.Verify(payload, signature, _legacyKeyId) == true;
    }

    internal bool RequiresSignature => _sign && _options.RejectUnsignedMessages;

    internal bool AcceptUnverifiedSignature()
    {
        if (_verify)
            return false;
        if (Interlocked.Exchange(ref _reportedUnverifiedSignature, 1) == 0)
            LogUnverifiedSignature(_logger);
        return true;
    }

    [LoggerMessage(
        Level = LogLevel.Information,
        Message = "A signed message was received while signature verification was disabled."
    )]
    private static partial void LogUnverifiedSignature(ILogger logger);

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        foreach (var key in _tenantKeys.Values)
            key.Dispose();
        _keys?.Dispose();
    }
}
