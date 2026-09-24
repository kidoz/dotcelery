using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.Migrations;

/// <summary>
/// The migrations for the tables owned by one store, applied with that store's
/// connection string and schema.
/// </summary>
public sealed class PostgresMigrationModule
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PostgresMigrationModule"/> class.
    /// </summary>
    /// <param name="name">
    /// The name recorded in the migration history. It must be unique within the schema.
    /// </param>
    /// <param name="connectionString">The connection string of the target database.</param>
    /// <param name="schema">The schema that holds the tables and the migration history.</param>
    /// <param name="migrations">The migrations, in increasing version order.</param>
    public PostgresMigrationModule(
        string name,
        string connectionString,
        string schema,
        IReadOnlyList<PostgresMigration> migrations
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        PostgresIdentifierValidator.ValidateConnectionString(
            connectionString,
            nameof(connectionString)
        );
        PostgresIdentifierValidator.ValidateIdentifier(schema, nameof(schema));
        ArgumentNullException.ThrowIfNull(migrations);

        long previousVersion = 0;
        foreach (var migration in migrations)
        {
            if (migration.Version <= previousVersion)
            {
                throw new ArgumentException(
                    $"Migration versions of module '{name}' must be positive and increasing.",
                    nameof(migrations)
                );
            }

            if (
                migration.Statements.Count == 0
                || migration.Statements.Any(string.IsNullOrWhiteSpace)
            )
            {
                throw new ArgumentException(
                    $"Migration {migration.Version} of module '{name}' has an empty statement.",
                    nameof(migrations)
                );
            }

            if (
                migration.Statements.Any(s =>
                    s.Contains(PostgresMigrator.BlockQuote, StringComparison.Ordinal)
                    || s.Contains(PostgresMigrator.StatementQuote, StringComparison.Ordinal)
                )
            )
            {
                throw new ArgumentException(
                    $"Migration {migration.Version} of module '{name}' contains a reserved dollar-quote tag.",
                    nameof(migrations)
                );
            }

            previousVersion = migration.Version;
        }

        Name = name;
        ConnectionString = connectionString;
        Schema = schema;
        Migrations = migrations;
    }

    /// <summary>
    /// Gets the name recorded in the migration history.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the connection string of the target database.
    /// </summary>
    public string ConnectionString { get; }

    /// <summary>
    /// Gets the schema that holds the tables and the migration history.
    /// </summary>
    public string Schema { get; }

    /// <summary>
    /// Gets the migrations, in increasing version order.
    /// </summary>
    public IReadOnlyList<PostgresMigration> Migrations { get; }
}
