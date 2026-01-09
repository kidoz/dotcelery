namespace DotCelery.Backend.Postgres.DelayedMessageStore;

/// <summary>
/// Options for <see cref="PostgresDelayedMessageStore"/>.
/// </summary>
public sealed class PostgresDelayedMessageStoreOptions
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
    public string TableName { get; set; } = "dotcelery_delayed_messages";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the batch size for fetching due messages.
    /// </summary>
    public int BatchSize { get; set; } = 100;
}
