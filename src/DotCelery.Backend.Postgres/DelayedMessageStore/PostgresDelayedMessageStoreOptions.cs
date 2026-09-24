using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.DelayedMessageStore;

/// <summary>
/// Options for <see cref="PostgresDelayedMessageStore"/>.
/// </summary>
public sealed class PostgresDelayedMessageStoreOptions
{
    private string _connectionString = "Host=localhost;Database=dotcelery";
    private string _schema = "public";
    private string _tableName = "dotcelery_delayed_messages";
    private TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);
    private int _batchSize = 100;

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
    /// Gets or sets the batch size for fetching due messages.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the batch size is out of range.</exception>
    public int BatchSize
    {
        get => _batchSize;
        set
        {
            PostgresIdentifierValidator.ValidateBatchSize(value, nameof(BatchSize));
            _batchSize = value;
        }
    }
}
