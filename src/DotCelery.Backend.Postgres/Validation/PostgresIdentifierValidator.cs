using System.Text.RegularExpressions;

namespace DotCelery.Backend.Postgres.Validation;

/// <summary>
/// Validates PostgreSQL identifiers to prevent SQL injection.
/// </summary>
public static partial class PostgresIdentifierValidator
{
    /// <summary>
    /// Maximum length for PostgreSQL identifiers.
    /// </summary>
    public const int MaxIdentifierLength = 63;

    /// <summary>
    /// Validates that a string is a safe PostgreSQL identifier.
    /// Valid identifiers start with a letter or underscore, followed by letters, digits, or underscores.
    /// </summary>
    /// <param name="identifier">The identifier to validate.</param>
    /// <returns>True if the identifier is valid; otherwise, false.</returns>
    public static bool IsValidIdentifier(string? identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            return false;
        }

        if (identifier.Length > MaxIdentifierLength)
        {
            return false;
        }

        return ValidIdentifierRegex().IsMatch(identifier);
    }

    /// <summary>
    /// Validates that a string is a safe PostgreSQL identifier and throws if not.
    /// </summary>
    /// <param name="identifier">The identifier to validate.</param>
    /// <param name="parameterName">The parameter name for the exception.</param>
    /// <exception cref="ArgumentException">Thrown when the identifier is invalid.</exception>
    public static void ValidateIdentifier(string? identifier, string parameterName)
    {
        if (!IsValidIdentifier(identifier))
        {
            throw new ArgumentException(
                $"Invalid PostgreSQL identifier. Must start with a letter or underscore, "
                    + $"contain only letters, digits, or underscores, and be at most {MaxIdentifierLength} characters.",
                parameterName
            );
        }
    }

    /// <summary>
    /// Validates that a connection string is not empty.
    /// </summary>
    /// <param name="connectionString">The connection string to validate.</param>
    /// <param name="parameterName">The parameter name for the exception.</param>
    /// <exception cref="ArgumentException">Thrown when the connection string is empty.</exception>
    public static void ValidateConnectionString(string? connectionString, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be empty.", parameterName);
        }
    }

    /// <summary>
    /// Validates that a timeout is positive and within reasonable bounds.
    /// </summary>
    /// <param name="timeout">The timeout to validate.</param>
    /// <param name="parameterName">The parameter name for the exception.</param>
    /// <param name="minTimeout">Minimum allowed timeout.</param>
    /// <param name="maxTimeout">Maximum allowed timeout.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the timeout is out of range.</exception>
    public static void ValidateTimeout(
        TimeSpan timeout,
        string parameterName,
        TimeSpan? minTimeout = null,
        TimeSpan? maxTimeout = null
    )
    {
        var min = minTimeout ?? TimeSpan.FromMilliseconds(100);
        var max = maxTimeout ?? TimeSpan.FromHours(1);

        if (timeout < min || timeout > max)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                timeout,
                $"Timeout must be between {min} and {max}."
            );
        }
    }

    /// <summary>
    /// Validates that a batch size is positive and within reasonable bounds.
    /// </summary>
    /// <param name="batchSize">The batch size to validate.</param>
    /// <param name="parameterName">The parameter name for the exception.</param>
    /// <param name="minSize">Minimum allowed size.</param>
    /// <param name="maxSize">Maximum allowed size.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the batch size is out of range.</exception>
    public static void ValidateBatchSize(
        int batchSize,
        string parameterName,
        int minSize = 1,
        int maxSize = 100000
    )
    {
        if (batchSize < minSize || batchSize > maxSize)
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                batchSize,
                $"Batch size must be between {minSize} and {maxSize}."
            );
        }
    }

    /// <summary>
    /// Regex pattern for valid PostgreSQL identifiers.
    /// Must start with a letter or underscore, followed by letters, digits, or underscores.
    /// </summary>
    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$", RegexOptions.Compiled)]
    private static partial Regex ValidIdentifierRegex();
}
