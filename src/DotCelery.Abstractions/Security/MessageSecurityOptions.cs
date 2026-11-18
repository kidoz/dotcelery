using System.Security.Cryptography;

namespace DotCelery.Core.Security;

/// <summary>
/// Security options for message handling.
/// </summary>
public sealed class MessageSecurityOptions
{
    /// <summary>
    /// Gets or sets the maximum payload size in bytes.
    /// Messages exceeding this limit will be rejected.
    /// Default is 1 MB (1,048,576 bytes).
    /// Set to 0 to disable limit.
    /// </summary>
    public int MaxPayloadSizeBytes { get; set; } = 1_048_576;

    /// <summary>
    /// Gets or sets whether to enforce the task name allowlist.
    /// When true, only tasks in <see cref="AllowedTaskNames"/> can be executed.
    /// Default is false (allow all tasks).
    /// </summary>
    public bool EnforceTaskAllowlist { get; set; }

    /// <summary>
    /// Gets or sets the set of allowed task names.
    /// Only used when <see cref="EnforceTaskAllowlist"/> is true.
    /// Task names should match the values in <c>[CeleryTask("name")]</c> attributes.
    /// </summary>
    public HashSet<string> AllowedTaskNames { get; set; } = [];

    /// <summary>
    /// Gets or sets whether to enable message signing.
    /// When true, messages are signed with HMAC-SHA256 and verified on receipt.
    /// </summary>
    public bool EnableMessageSigning { get; set; }

    /// <summary>Gets or sets whether to verify signed deliveries when local signing is disabled.</summary>
    /// <remarks>Unsigned messages remain accepted in this mode.</remarks>
    public bool VerifySignaturesWhenPresent { get; set; }

    /// <summary>
    /// Gets or sets the secret key for message signing.
    /// Required when <see cref="EnableMessageSigning"/> is true.
    /// Should be at least 32 bytes for security.
    /// </summary>
    public byte[]? SigningKey { get; set; }

    /// <summary>Gets or sets the ID sent with newly signed messages. Null writes legacy signatures.</summary>
    public string? SigningKeyId { get; set; }

    /// <summary>Gets or sets the additional accepted verification keys, indexed by key ID.</summary>
    public Dictionary<string, byte[]> AdditionalVerificationKeys { get; set; } =
        new(StringComparer.Ordinal);

    /// <summary>Gets or sets the key ID used to check legacy signatures that carry no key ID.</summary>
    /// <remarks>Without an ID, legacy signatures are checked with the active key.</remarks>
    public string? LegacyVerificationKeyId { get; set; }

    /// <summary>
    /// Gets or sets independent HMAC keys keyed by explicit TenantId. With this map populated,
    /// signing and verification never fall back to the process-wide key.
    /// </summary>
    public Dictionary<string, byte[]> TenantSigningKeys { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Gets or sets the active key IDs by tenant. Missing entries write legacy signatures.</summary>
    public Dictionary<string, string> TenantSigningKeyIds { get; set; } =
        new(StringComparer.Ordinal);

    /// <summary>Gets or sets additional verification keys by tenant and then key ID.</summary>
    public Dictionary<
        string,
        Dictionary<string, byte[]>
    > TenantAdditionalVerificationKeys { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets or sets the signing key as a Base64 string.
    /// Convenience property for configuration.
    /// </summary>
    public string? SigningKeyBase64
    {
        get => SigningKey is not null ? Convert.ToBase64String(SigningKey) : null;
        set => SigningKey = value is not null ? Convert.FromBase64String(value) : null;
    }

    /// <summary>
    /// Gets or sets whether to reject messages with invalid or missing signatures.
    /// When false, unsigned messages are allowed but logged as warnings.
    /// Default is true (strict mode).
    /// </summary>
    public bool RejectUnsignedMessages { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum allowed schema version.
    /// Messages with a schema version higher than this are rejected.
    /// Default is the current schema version.
    /// </summary>
    public int MaxAllowedSchemaVersion { get; set; } = Models.TaskMessage.CurrentSchemaVersion;

    /// <summary>
    /// Gets or sets the age beyond which a signed message is refused, or <c>null</c> to accept
    /// messages of any age (default). It narrows the window in which a captured signed message
    /// can be replayed.
    /// </summary>
    /// <remarks>
    /// A message is delivered again after a worker restarts, a claim expires, or a retry, and it
    /// keeps the timestamp it was sent with, so a window that is too short refuses those
    /// redeliveries, which are then dead-lettered rather than run. Prefer
    /// <c>UseInboxDeduplication</c> when a captured message must not be replayed at all: it
    /// refuses a message whose ID was already processed, whatever its age.
    /// </remarks>
    public TimeSpan? MaxMessageAge { get; set; }

    /// <summary>Gets or sets the future clock skew allowed when <see cref="MaxMessageAge"/> is set.</summary>
    public TimeSpan MaxClockSkew { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Generates a cryptographically random 32-byte HMAC signing key.</summary>
    /// <returns>The key bytes.</returns>
    public static byte[] GenerateKey() => RandomNumberGenerator.GetBytes(32);

    /// <summary>
    /// Validates the security options.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when options are invalid.</exception>
    public void Validate()
    {
        if (
            EnableMessageSigning
            && TenantSigningKeys.Count == 0
            && (SigningKey is null || SigningKey.Length < 16)
        )
        {
            throw new InvalidOperationException(
                "Message signing is enabled but SigningKey is not set or is too short. "
                    + "Provide a key of at least 16 bytes (32 bytes recommended)."
            );
        }

        foreach (var (tenant, key) in TenantSigningKeys)
        {
            if (string.IsNullOrWhiteSpace(tenant) || key.Length < 16)
                throw new InvalidOperationException(
                    "Each tenant needs an explicit ID and an independent signing key of at least 16 bytes."
                );
        }
        foreach (var (tenant, id) in TenantSigningKeyIds)
        {
            if (!TenantSigningKeys.ContainsKey(tenant) || !IsValidKeyId(id))
                throw new InvalidOperationException(
                    "A tenant signing key ID requires an active key and a valid ID."
                );
        }

        if (SigningKeyId is not null && (SigningKey is null || !IsValidKeyId(SigningKeyId)))
            throw new InvalidOperationException(
                "SigningKeyId requires an active signing key and a valid ID."
            );
        if (LegacyVerificationKeyId is not null && !IsValidKeyId(LegacyVerificationKeyId))
            throw new InvalidOperationException("LegacyVerificationKeyId is invalid.");

        ValidateVerificationKeys(AdditionalVerificationKeys, SigningKeyId);
        foreach (var (tenant, verificationKeys) in TenantAdditionalVerificationKeys)
        {
            if (
                string.IsNullOrWhiteSpace(tenant)
                || (EnableMessageSigning && !TenantSigningKeys.ContainsKey(tenant))
            )
                throw new InvalidOperationException(
                    "Tenant verification keys require an explicit tenant and, when signing is enabled, an active key."
                );
            ValidateVerificationKeys(
                verificationKeys,
                TenantSigningKeyIds.GetValueOrDefault(tenant)
            );
        }

        var keys = TenantSigningKeys
            .Select(pair => (Tenant: pair.Key, Key: pair.Value))
            .Concat(
                TenantAdditionalVerificationKeys.SelectMany(tenant =>
                    tenant.Value.Values.Select(key => (Tenant: tenant.Key, Key: key))
                )
            )
            .ToArray();
        for (var i = 0; i < keys.Length; i++)
        for (var j = i + 1; j < keys.Length; j++)
            if (
                keys[i].Tenant != keys[j].Tenant
                && CryptographicOperations.FixedTimeEquals(keys[i].Key, keys[j].Key)
            )
                throw new InvalidOperationException("Tenant signing keys must be distinct.");

        if (MaxPayloadSizeBytes < 0)
        {
            throw new InvalidOperationException("MaxPayloadSizeBytes cannot be negative.");
        }

        if (MaxMessageAge < TimeSpan.Zero || MaxClockSkew < TimeSpan.Zero)
            throw new InvalidOperationException(
                "Message age and clock skew limits cannot be negative."
            );
        if (MaxMessageAge > TimeSpan.MaxValue - MaxClockSkew)
            throw new InvalidOperationException(
                "The message age and clock skew limits are too large when combined."
            );
    }

    private static void ValidateVerificationKeys(
        Dictionary<string, byte[]> keys,
        string? activeKeyId
    )
    {
        foreach (var (id, key) in keys)
            if (!IsValidKeyId(id) || id == activeKeyId || key.Length < 16)
                throw new InvalidOperationException(
                    "Additional verification keys need a unique key ID and at least 16 bytes."
                );
    }

    private static bool IsValidKeyId(string id) =>
        id.Length is > 0 and <= 64
        && id.All(character =>
            char.IsAsciiLetterOrDigit(character) || character is '-' or '_' or '.'
        );
}
