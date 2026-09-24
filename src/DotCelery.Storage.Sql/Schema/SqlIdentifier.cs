using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace DotCelery.Storage.Sql.Schema;

/// <summary>
/// Validates schema, table, column, index, and sequence names.
/// </summary>
/// <remarks>
/// Names must start with a letter or underscore, contain only letters, digits, and
/// underscores, and be at most 63 characters, so that every supported database accepts
/// them and they can never carry SQL.
/// </remarks>
public static partial class SqlIdentifier
{
    /// <summary>
    /// The maximum identifier length.
    /// </summary>
    public const int MaxLength = 63;

    /// <summary>
    /// Returns whether <paramref name="name"/> is a valid identifier.
    /// </summary>
    /// <param name="name">The name to check.</param>
    /// <returns><c>true</c> if the name is valid.</returns>
    public static bool IsValid(string? name) =>
        !string.IsNullOrEmpty(name) && name.Length <= MaxLength && Pattern().IsMatch(name);

    /// <summary>
    /// Throws if <paramref name="name"/> is not a valid identifier.
    /// </summary>
    /// <param name="name">The name to check.</param>
    /// <param name="paramName">The parameter name, captured automatically.</param>
    public static void ThrowIfInvalid(
        string? name,
        [CallerArgumentExpression(nameof(name))] string? paramName = null
    )
    {
        if (!IsValid(name))
        {
            throw new ArgumentException(
                $"'{name}' is not a valid SQL identifier. Use letters, digits, and underscores, "
                    + $"start with a letter or underscore, and use at most {MaxLength} characters.",
                paramName
            );
        }
    }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex Pattern();
}
