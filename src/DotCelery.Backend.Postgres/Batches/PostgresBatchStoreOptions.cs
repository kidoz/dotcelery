namespace DotCelery.Backend.Postgres.Batches;

/// <summary>
/// Options for <see cref="PostgresBatchStore"/>.
/// </summary>
public sealed class PostgresBatchStoreOptions
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
    /// Gets or sets the batches table name.
    /// </summary>
    public string BatchesTableName { get; set; } = "dotcelery_batches";

    /// <summary>
    /// Gets or sets the batch tasks table name.
    /// </summary>
    public string BatchTasksTableName { get; set; } = "dotcelery_batch_tasks";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
