using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.Metrics;

/// <summary>
/// Options for <see cref="PostgresQueueMetrics"/>.
/// </summary>
public sealed class PostgresQueueMetricsOptions
{
    private string _connectionString = "Host=localhost;Database=dotcelery";
    private string _schema = "public";
    private string _metricsTableName = "dotcelery_queue_metrics";
    private string _runningTasksTableName = "dotcelery_running_tasks";
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
    /// Gets or sets the queue metrics table name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the table name is invalid.</exception>
    public string MetricsTableName
    {
        get => _metricsTableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(MetricsTableName));
            _metricsTableName = value;
        }
    }

    /// <summary>
    /// Gets or sets the running tasks table name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the table name is invalid.</exception>
    public string RunningTasksTableName
    {
        get => _runningTasksTableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(RunningTasksTableName));
            _runningTasksTableName = value;
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
