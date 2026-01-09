namespace DotCelery.Backend.Postgres.DeadLetter;

/// <summary>
/// Options for <see cref="PostgresDeadLetterStore"/>.
/// </summary>
public sealed class PostgresDeadLetterStoreOptions
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
    public string TableName { get; set; } = "dotcelery_dead_letters";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the default retention period.
    /// </summary>
    public TimeSpan DefaultRetention { get; set; } = TimeSpan.FromDays(30);
}
