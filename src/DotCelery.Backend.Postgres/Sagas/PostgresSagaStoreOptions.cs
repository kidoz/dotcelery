namespace DotCelery.Backend.Postgres.Sagas;

/// <summary>
/// Options for <see cref="PostgresSagaStore"/>.
/// </summary>
public sealed class PostgresSagaStoreOptions
{
    /// <summary>
    /// Gets or sets the PostgreSQL connection string.
    /// </summary>
    public string ConnectionString { get; set; } = "Host=localhost;Database=dotcelery";

    /// <summary>
    /// Gets or sets the schema name.
    /// </summary>
    public string Schema { get; set; } = "public";

    /// <summary>
    /// Gets or sets the sagas table name.
    /// </summary>
    public string SagasTableName { get; set; } = "dotcelery_sagas";

    /// <summary>
    /// Gets or sets the saga steps table name.
    /// </summary>
    public string SagaStepsTableName { get; set; } = "dotcelery_saga_steps";

    /// <summary>
    /// Gets or sets the task-to-saga mapping table name.
    /// </summary>
    public string TaskSagaTableName { get; set; } = "dotcelery_task_sagas";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
