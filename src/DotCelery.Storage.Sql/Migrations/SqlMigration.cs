using System.Security.Cryptography;
using System.Text;
using DotCelery.Storage.Sql.Schema;

namespace DotCelery.Storage.Sql.Migrations;

/// <summary>
/// A forward-only schema change. Its operations run in one transaction together with the
/// history record, so a migration is either fully applied or not applied at all.
/// </summary>
public sealed record SqlMigration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlMigration"/> class.
    /// </summary>
    /// <param name="version">The version within its module; positive and increasing.</param>
    /// <param name="description">What the migration changes.</param>
    /// <param name="operations">The schema operations, applied in order.</param>
    public SqlMigration(long version, string description, IReadOnlyList<SchemaOperation> operations)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentException.ThrowIfNullOrWhiteSpace(description);
        ArgumentNullException.ThrowIfNull(operations);
        if (operations.Count == 0)
        {
            throw new ArgumentException(
                $"Migration {version} has no operations.",
                nameof(operations)
            );
        }

        Version = version;
        Description = description;
        Operations = operations;
    }

    /// <summary>Gets the version within its module.</summary>
    public long Version { get; }

    /// <summary>Gets what the migration changes.</summary>
    public string Description { get; }

    /// <summary>Gets the schema operations, applied in order.</summary>
    public IReadOnlyList<SchemaOperation> Operations { get; }

    /// <summary>
    /// Gets the checksum recorded when the migration is applied. It covers the operations,
    /// not the SQL a dialect renders for them.
    /// </summary>
    public string Checksum =>
        Convert.ToHexStringLower(
            SHA256.HashData(
                Encoding.UTF8.GetBytes(string.Join("\n", Operations.Select(o => o.Describe())))
            )
        );
}

/// <summary>
/// The migrations for the tables owned by one component, applied with that component's
/// connection string and schema.
/// </summary>
public sealed class SqlMigrationModule
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SqlMigrationModule"/> class.
    /// </summary>
    /// <param name="name">The name recorded in the migration history; unique within the schema.</param>
    /// <param name="connectionString">The connection string of the target database.</param>
    /// <param name="schema">The schema that holds the tables and the migration history.</param>
    /// <param name="migrations">The migrations, in increasing version order.</param>
    public SqlMigrationModule(
        string name,
        string connectionString,
        string schema,
        IReadOnlyList<SqlMigration> migrations
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        SqlIdentifier.ThrowIfInvalid(schema);
        ArgumentNullException.ThrowIfNull(migrations);

        long previousVersion = 0;
        foreach (var migration in migrations)
        {
            if (migration.Version <= previousVersion)
            {
                throw new ArgumentException(
                    $"Migration versions of module '{name}' must be increasing.",
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

    /// <summary>Gets the name recorded in the migration history.</summary>
    public string Name { get; }

    /// <summary>Gets the connection string of the target database.</summary>
    public string ConnectionString { get; }

    /// <summary>Gets the schema that holds the tables and the migration history.</summary>
    public string Schema { get; }

    /// <summary>Gets the migrations, in increasing version order.</summary>
    public IReadOnlyList<SqlMigration> Migrations { get; }
}
