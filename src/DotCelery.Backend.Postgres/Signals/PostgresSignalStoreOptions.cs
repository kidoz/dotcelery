using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.Signals;

/// <summary>
/// Configuration options for <see cref="PostgresSignalStore"/>.
/// </summary>
public sealed class PostgresSignalStoreOptions
{
    private string _connectionString = "Host=localhost;Database=celery";
    private string _schema = "public";
    private string _tableName = "celery_signals";
    private TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the PostgreSQL connection string.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the connection string is empty.</exception>
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
    /// Gets or sets the schema name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the schema name is invalid.</exception>
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
    /// Gets or sets the table name for signal messages.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the table name is invalid.</exception>
    public string TableName
    {
        get => _tableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(TableName));
            _tableName = value;
        }
    }

    /// <summary>
    /// Gets or sets whether to automatically create tables on startup.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the timeout is out of range.</exception>
    public TimeSpan CommandTimeout
    {
        get => _commandTimeout;
        set
        {
            PostgresIdentifierValidator.ValidateTimeout(value, nameof(CommandTimeout));
            _commandTimeout = value;
        }
    }

    /// <summary>
    /// Gets or sets the visibility timeout for processing messages.
    /// Messages will become visible again if not acknowledged within this time.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
