namespace DotCelery.Backend.Postgres.Migrations;

/// <summary>
/// A forward-only schema change. Its statements run in one transaction together with
/// the history record, so a migration is either fully applied or not applied at all.
/// </summary>
/// <param name="Version">The version within its module. Versions must be positive and increasing.</param>
/// <param name="Description">What the migration changes.</param>
/// <param name="Statements">The SQL statements to run, in order.</param>
public sealed record PostgresMigration(
    long Version,
    string Description,
    IReadOnlyList<string> Statements
);
