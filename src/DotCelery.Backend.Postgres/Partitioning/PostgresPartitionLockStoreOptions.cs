namespace DotCelery.Backend.Postgres.Partitioning;

/// <summary>
/// Options for <see cref="PostgresPartitionLockStore"/>.
/// </summary>
public sealed class PostgresPartitionLockStoreOptions
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
    /// Gets or sets the table name.
    /// </summary>
    public string TableName { get; set; } = "dotcelery_partition_locks";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
