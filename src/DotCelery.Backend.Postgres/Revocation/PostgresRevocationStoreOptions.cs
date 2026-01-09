namespace DotCelery.Backend.Postgres.Revocation;

/// <summary>
/// Options for <see cref="PostgresRevocationStore"/>.
/// </summary>
public sealed class PostgresRevocationStoreOptions
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
    public string TableName { get; set; } = "dotcelery_revocations";

    /// <summary>
    /// Gets or sets the notify channel for pub/sub.
    /// </summary>
    public string NotifyChannel { get; set; } = "dotcelery_revocation";

    /// <summary>
    /// Gets or sets whether to auto-create tables.
    /// </summary>
    public bool AutoCreateTables { get; set; } = true;

    /// <summary>
    /// Gets or sets the command timeout.
    /// </summary>
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the default retention period for revocations.
    /// </summary>
    public TimeSpan DefaultRetention { get; set; } = TimeSpan.FromDays(7);
}
