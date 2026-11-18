using System.Security.Cryptography;

namespace DotCelery.Core.Security;

internal sealed class MessageSigningKeys(
    byte[]? key,
    string? keyId,
    Dictionary<string, byte[]>? additionalKeys
) : IDisposable
{
    private readonly byte[]? _activeKey = key?.ToArray();
    private readonly Dictionary<string, byte[]> _additionalKeys = additionalKeys is null
        ? new(StringComparer.Ordinal)
        : additionalKeys.ToDictionary(
            pair => pair.Key,
            pair => pair.Value.ToArray(),
            StringComparer.Ordinal
        );

    public string? Sign(byte[] payload)
    {
        if (_activeKey is null)
            return null;
        var hash = Convert.ToBase64String(HMACSHA256.HashData(_activeKey, payload));
        return keyId is null ? hash : $"{keyId}:{hash}";
    }

    public bool Verify(byte[] payload, string signature, string? legacyKeyId)
    {
        if (signature.Length > 109)
            return false;
        var separator = signature.IndexOf(':');
        if (separator is 0 or > 64)
            return false;
        var id = separator < 0 ? legacyKeyId : signature[..separator];
        var verificationKey =
            id is null || id == keyId ? _activeKey : _additionalKeys.GetValueOrDefault(id);
        if (verificationKey is null)
            return false;

        Span<byte> actual = stackalloc byte[32];
        if (
            !Convert.TryFromBase64Chars(signature.AsSpan(separator + 1), actual, out var written)
            || written != actual.Length
        )
            return false;
        Span<byte> expected = stackalloc byte[32];
        HMACSHA256.HashData(verificationKey, payload, expected);
        return CryptographicOperations.FixedTimeEquals(expected, actual);
    }

    public void Dispose()
    {
        if (_activeKey is not null)
            CryptographicOperations.ZeroMemory(_activeKey);
        foreach (var verificationKey in _additionalKeys.Values)
            CryptographicOperations.ZeroMemory(verificationKey);
    }
}
