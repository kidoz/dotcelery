using DotCelery.Backend.Postgres.Validation;
using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Execution;
using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Storage;
using Npgsql;

namespace DotCelery.Backend.Postgres.Storage;

/// <summary>
/// Options for the PostgreSQL storage primitives.
/// </summary>
public sealed class PostgresStorageOptions
{
    private string _connectionString = "Host=localhost;Database=celery";
    private string _schema = "public";
    private TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the connection string.
    /// </summary>
    public string ConnectionString
    {
        get => _connectionString;
        set
        {
            PostgresIdentifierValidator.ValidateConnectionString(value, nameof(ConnectionString));
            _connectionString = value;
        }
    }

    /// <summary>
    /// Gets or sets the schema of the storage tables.
    /// </summary>
    public string Schema
    {
        get => _schema;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(Schema));
            _schema = value;
        }
    }

    /// <summary>
    /// Gets or sets the timeout for each statement.
    /// </summary>
    public TimeSpan CommandTimeout
    {
        get => _commandTimeout;
        set
        {
            PostgresIdentifierValidator.ValidateTimeout(value, nameof(CommandTimeout));
            _commandTimeout = value;
        }
    }
}

/// <summary>
/// Creates the PostgreSQL storage primitives and their migrations.
/// </summary>
public static class PostgresStorage
{
    /// <summary>
    /// Creates the storage provider. Its tables must exist; apply the migrations from
    /// <see cref="CreateModule"/> first.
    /// </summary>
    /// <param name="dataSource">The data source of the database.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="timeProvider">The clock for expiry, due times, and leases.</param>
    /// <returns>The storage provider.</returns>
    public static IStorageProvider CreateProvider(
        NpgsqlDataSource dataSource,
        PostgresStorageOptions options,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(options);

        return new SqlStorageProvider(
            "PostgreSQL",
            new PostgresStorageStatements(options.Schema),
            new SqlExecutor(dataSource, options.CommandTimeout),
            timeProvider,
            new PostgresNotificationChannel(dataSource)
        );
    }

    /// <summary>
    /// Creates the migration module for the storage tables.
    /// </summary>
    /// <param name="options">The storage options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(PostgresStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return SqlStorageSchema.CreateModule(options.ConnectionString, options.Schema);
    }
}
