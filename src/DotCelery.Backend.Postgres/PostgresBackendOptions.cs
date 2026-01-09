using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres;

/// <summary>
/// Configuration options for the PostgreSQL result backend.
/// </summary>
public sealed class PostgresBackendOptions
{
    private string _connectionString = "Host=localhost;Database=celery";
    private string _tableName = "celery_task_results";
    private string _schema = "public";
    private string _notifyChannelPrefix = "celery_task_done_";
    private TimeSpan _pollingInterval = TimeSpan.FromMilliseconds(100);
    private TimeSpan _commandTimeout = TimeSpan.FromSeconds(30);
    private int _cleanupBatchSize = 1000;

    /// <summary>
    /// Gets or sets the PostgreSQL connection string.
    /// Example: "Host=localhost;Database=celery;Username=celery;Password=secret"
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
    /// Gets or sets the table name for task results.
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
    /// Gets or sets the default result expiry time.
    /// </summary>
    public TimeSpan DefaultExpiry { get; set; } = TimeSpan.FromDays(1);

    /// <summary>
    /// Gets or sets the polling interval when waiting for results.
    /// Used as fallback when LISTEN/NOTIFY fails or is disabled.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the interval is out of range.</exception>
    public TimeSpan PollingInterval
    {
        get => _pollingInterval;
        set
        {
            PostgresIdentifierValidator.ValidateTimeout(
                value,
                nameof(PollingInterval),
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMinutes(5)
            );
            _pollingInterval = value;
        }
    }

    /// <summary>
    /// Gets or sets whether to use PostgreSQL LISTEN/NOTIFY for result notifications.
    /// </summary>
    public bool UseListenNotify { get; set; } = true;

    /// <summary>
    /// Gets or sets the notification channel prefix for LISTEN/NOTIFY.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the prefix is invalid.</exception>
    public string NotifyChannelPrefix
    {
        get => _notifyChannelPrefix;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(NotifyChannelPrefix));
            _notifyChannelPrefix = value;
        }
    }

    /// <summary>
    /// Gets or sets whether to automatically create tables on startup.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the cleanup interval for expired results.
    /// Set to null to disable automatic cleanup.
    /// </summary>
    public TimeSpan? CleanupInterval { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// Gets or sets the batch size for cleanup operations.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the batch size is out of range.</exception>
    public int CleanupBatchSize
    {
        get => _cleanupBatchSize;
        set
        {
            PostgresIdentifierValidator.ValidateBatchSize(value, nameof(CleanupBatchSize));
            _cleanupBatchSize = value;
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
}
