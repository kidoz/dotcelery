using System.Runtime.CompilerServices;

namespace DotCelery.Core.Storage;

/// <summary>
/// Argument checks shared by storage providers, so that every provider accepts the same input.
/// </summary>
public static class StorageGuard
{
    /// <summary>
    /// The maximum length of a collection, queue, or channel name.
    /// </summary>
    public const int MaxNameLength = 128;

    /// <summary>
    /// The maximum length of a document id, index key, lease key, or counter key.
    /// </summary>
    public const int MaxKeyLength = 512;

    /// <summary>
    /// Throws if <paramref name="name"/> is not a valid collection, queue, or channel name.
    /// </summary>
    /// <param name="name">The name to check.</param>
    /// <param name="paramName">The parameter name, captured automatically.</param>
    public static void ThrowIfInvalidName(
        string name,
        [CallerArgumentExpression(nameof(name))] string? paramName = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name, paramName);

        if (name.Length > MaxNameLength)
        {
            throw new ArgumentException($"Must be at most {MaxNameLength} characters.", paramName);
        }
    }

    /// <summary>
    /// Throws if <paramref name="key"/> is not a valid id or key.
    /// </summary>
    /// <param name="key">The key to check.</param>
    /// <param name="paramName">The parameter name, captured automatically.</param>
    public static void ThrowIfInvalidKey(
        string key,
        [CallerArgumentExpression(nameof(key))] string? paramName = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key, paramName);
        ThrowIfTooLong(key, paramName);
    }

    /// <summary>
    /// Throws if <paramref name="key"/> is set and is not a valid key.
    /// </summary>
    /// <param name="key">The key to check.</param>
    /// <param name="paramName">The parameter name, captured automatically.</param>
    public static void ThrowIfInvalidOptionalKey(
        string? key,
        [CallerArgumentExpression(nameof(key))] string? paramName = null
    )
    {
        if (key is not null)
        {
            ThrowIfInvalidKey(key, paramName);
        }
    }

    /// <summary>
    /// Throws if <paramref name="prefix"/> is not a valid key prefix. An empty prefix matches all keys.
    /// </summary>
    /// <param name="prefix">The prefix to check.</param>
    /// <param name="paramName">The parameter name, captured automatically.</param>
    public static void ThrowIfInvalidPrefix(
        string prefix,
        [CallerArgumentExpression(nameof(prefix))] string? paramName = null
    )
    {
        ArgumentNullException.ThrowIfNull(prefix, paramName);
        ThrowIfTooLong(prefix, paramName);
    }

    /// <summary>
    /// Throws if <paramref name="value"/> is not greater than zero.
    /// </summary>
    /// <param name="value">The duration to check.</param>
    /// <param name="paramName">The parameter name, captured automatically.</param>
    public static void ThrowIfNotPositive(
        TimeSpan value,
        [CallerArgumentExpression(nameof(value))] string? paramName = null
    )
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(paramName, value, "Must be greater than zero.");
        }
    }

    private static void ThrowIfTooLong(string value, string? paramName)
    {
        if (value.Length > MaxKeyLength)
        {
            throw new ArgumentException($"Must be at most {MaxKeyLength} characters.", paramName);
        }
    }
}
