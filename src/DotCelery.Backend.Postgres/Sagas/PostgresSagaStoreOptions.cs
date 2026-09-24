using DotCelery.Backend.Postgres.Validation;

namespace DotCelery.Backend.Postgres.Sagas;

/// <summary>
/// Options for <see cref="PostgresSagaStore"/>.
/// </summary>
public sealed class PostgresSagaStoreOptions
{
    private string _connectionString = "Host=localhost;Database=dotcelery";
    private string _schema = "public";
    private string _sagasTableName = "dotcelery_sagas";
    private string _sagaStepsTableName = "dotcelery_saga_steps";
    private string _taskSagaTableName = "dotcelery_task_sagas";
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
    /// Gets or sets the sagas table name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the table name is invalid.</exception>
    public string SagasTableName
    {
        get => _sagasTableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(SagasTableName));
            _sagasTableName = value;
        }
    }

    /// <summary>
    /// Gets or sets the saga steps table name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the table name is invalid.</exception>
    public string SagaStepsTableName
    {
        get => _sagaStepsTableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(SagaStepsTableName));
            _sagaStepsTableName = value;
        }
    }

    /// <summary>
    /// Gets or sets the task-to-saga mapping table name.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the table name is invalid.</exception>
    public string TaskSagaTableName
    {
        get => _taskSagaTableName;
        set
        {
            PostgresIdentifierValidator.ValidateIdentifier(value, nameof(TaskSagaTableName));
            _taskSagaTableName = value;
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
