using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.RateLimiting;

/// <summary>
/// Options for <see cref="PostgresRateLimiter"/>.
/// </summary>
public sealed class PostgresRateLimiterOptions
{
    private string _connectionString = "Host=localhost;Database=dotcelery";
    private string _schema = "public";
    private string _tableName = "dotcelery_rate_limits";
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
    /// Gets or sets the table name.
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
    /// Gets or sets whether to auto-create tables.
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
}
