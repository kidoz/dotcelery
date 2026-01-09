using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.Outbox;

/// <summary>
/// Configuration options for <see cref="PostgresOutboxStore"/>.
/// </summary>
public sealed class PostgresOutboxStoreOptions
{
    private string _connectionString = "Host=localhost;Database=celery";
    private string _schema = "public";
    private string _tableName = "celery_outbox";
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
    /// Gets or sets the table name for outbox messages.
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
    /// Gets or sets the default TTL for dispatched messages before cleanup.
    /// </summary>
    public TimeSpan DispatchedMessageTtl { get; set; } = TimeSpan.FromDays(7);
}
