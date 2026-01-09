namespace DotCelery.Core.Migrations;

/// <summary>
/// Stores migration history to track which migrations have been applied.
/// Implementations exist for SQL tables, Redis keys, MongoDB collections, etc.
/// </summary>
public interface IMigrationStore
{
    /// <summary>
    /// Ensures the migration store is initialized (creates tracking table/collection if needed).
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets all applied migration versions in ascending order.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of applied migration versions.</returns>
    Task<IReadOnlyList<long>> GetAppliedVersionsAsync(
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Marks a migration as applied.
    /// </summary>
    /// <param name="version">The migration version.</param>
    /// <param name="description">The migration description.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask MarkAppliedAsync(
        long version,
        string description,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Marks a migration as rolled back (removes from applied list).
    /// </summary>
    /// <param name="version">The migration version.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    ValueTask MarkRolledBackAsync(long version, CancellationToken cancellationToken = default);
}

/// <summary>
/// Record of an applied migration.
/// </summary>
public sealed record MigrationRecord
{
    /// <summary>
    /// Gets the migration version.
    /// </summary>
    public required long Version { get; init; }

    /// <summary>
    /// Gets the migration description.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// Gets when the migration was applied.
    /// </summary>
    public required DateTimeOffset AppliedAt { get; init; }
}
