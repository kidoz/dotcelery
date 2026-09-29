using System.Data.Common;
using DotCelery.Core.Storage;
using DotCelery.Storage.Sql.Execution;
using DotCelery.Storage.Sql.Migrations;
using DotCelery.Storage.Sql.Schema;
using DotCelery.Storage.Sql.Storage;
using Microsoft.Data.SqlClient;

namespace DotCelery.Backend.SqlServer.Storage;

/// <summary>
/// Options for the SQL Server storage primitives.
/// </summary>
public sealed class SqlServerStorageOptions
{
    private string _connectionString =
        "Server=localhost;Database=dotcelery;Integrated Security=true;TrustServerCertificate=true";
    private string _schema = "dbo";
    private TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the connection string.
    /// </summary>
    public string ConnectionString
    {
        get => _connectionString;
        set
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            _connectionString = value;
        }
    }

    /// <summary>
    /// Gets or sets the schema of the storage tables. Default is <c>dbo</c>.
    /// </summary>
    public string Schema
    {
        get => _schema;
        set
        {
            SqlIdentifier.ThrowIfInvalid(value);
            _schema = value;
        }
    }

    /// <summary>
    /// Gets or sets how long a command may run. Default is 30 seconds.
    /// </summary>
    public TimeSpan CommandTimeout
    {
        get => _commandTimeout;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            _commandTimeout = value;
        }
    }
}

/// <summary>
/// Creates the SQL Server storage primitives and the migrations for their tables.
/// </summary>
/// <remarks>
/// SQL Server has no simple publish/subscribe, so the provider has no notifications and the
/// stores poll.
/// </remarks>
public static class SqlServerStorage
{
    /// <summary>
    /// Creates the storage provider.
    /// </summary>
    /// <param name="dataSource">The data source of the database.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="timeProvider">The clock for expiry, due times, and lease deadlines.</param>
    /// <returns>The storage provider.</returns>
    public static IStorageProvider CreateProvider(
        DbDataSource dataSource,
        SqlServerStorageOptions options,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(options);

        return new SqlStorageProvider(
            "SQL Server",
            new SqlServerStorageStatements(options.Schema),
            new SqlExecutor(dataSource, options.CommandTimeout, IsTransient),
            timeProvider
        );
    }

    // A deadlock victim's transaction was rolled back, so it can run again
    private static bool IsTransient(DbException exception) =>
        exception is SqlException { Number: 1205 };

    /// <summary>
    /// Creates the migration module for the storage tables.
    /// </summary>
    /// <param name="options">The storage options.</param>
    /// <returns>The migration module.</returns>
    public static SqlMigrationModule CreateModule(SqlServerStorageOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return SqlStorageSchema.CreateModule(options.ConnectionString, options.Schema);
    }
}
