using DotCelery.Backend.Postgres.Validation;
using DotCelery.Core.Dashboard;

namespace DotCelery.Backend.Postgres.Historical;

/// <summary>
/// Options for <see cref="PostgresHistoricalDataStore"/>.
/// </summary>
public sealed class PostgresHistoricalDataStoreOptions
{
    private string _connectionString = "Host=localhost;Database=dotcelery";
    private string _schema = "public";
    private string _snapshotsTableName = "dotcelery_metrics_snapshots";
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
    /// Gets or sets the snapshots table name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the table name is invalid.</exception>
    public string SnapshotsTableName
    {
        get => _snapshotsTableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(SnapshotsTableName));
            _snapshotsTableName = value;
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
    /// Gets or sets the retention period.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(30);

    /// <summary>
    /// Gets or sets the maximum number of data points for time series queries.
    /// </summary>
    public int MaxDataPoints { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the default granularity for queries.
    /// </summary>
    public MetricsGranularity DefaultGranularity { get; set; } = MetricsGranularity.Hour;
}
