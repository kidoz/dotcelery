namespace DotCelery.Core.Migrations;

/// <summary>
/// Represents a database migration that can be applied or rolled back.
/// Works with both SQL and NoSQL databases.
/// </summary>
public interface IMigration
{
    /// <summary>
    /// Gets the unique version number of this migration.
    /// Use timestamp format (e.g., 20250109001) for ordering.
    /// </summary>
    long Version { get; }

    /// <summary>
    /// Gets a human-readable description of what this migration does.
    /// </summary>
    string Description { get; }

    /// <summary>
    /// Applies the migration (upgrade).
    /// </summary>
    /// <param name="context">The migration context providing database access.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask UpAsync(IMigrationContext context, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reverts the migration (downgrade).
    /// </summary>
    /// <param name="context">The migration context providing database access.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask DownAsync(IMigrationContext context, CancellationToken cancellationToken = default);
}

/// <summary>
/// Provides context for migration execution.
/// Abstracted to support both SQL and NoSQL databases.
/// </summary>
public interface IMigrationContext
{
    /// <summary>
    /// Executes a raw command (SQL statement or NoSQL operation).
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ExecuteAsync(string command, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes a command with parameters.
    /// </summary>
    /// <param name="command">The command to execute.</param>
    /// <param name="parameters">Command parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask ExecuteAsync(
        string command,
        IDictionary<string, object?> parameters,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Checks if a structure exists (table, collection, key pattern, etc.).
    /// </summary>
    /// <param name="name">Name of the structure to check.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask<bool> ExistsAsync(string name, CancellationToken cancellationToken = default);
}
