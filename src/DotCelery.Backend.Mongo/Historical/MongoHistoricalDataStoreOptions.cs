namespace DotCelery.Backend.Mongo.Historical;

/// <summary>
/// Options for <see cref="MongoHistoricalDataStore"/>.
/// </summary>
public sealed class MongoHistoricalDataStoreOptions
{
    /// <summary>
    /// Gets or sets the MongoDB connection string.
    /// </summary>
    public string ConnectionString { get; set; } = "mongodb://localhost:27017";

    /// <summary>
    /// Gets or sets the database name.
    /// </summary>
    public string DatabaseName { get; set; } = "dotcelery";

    /// <summary>
    /// Gets or sets the collection name for metrics snapshots.
    /// </summary>
    public string CollectionName { get; set; } = "historical_metrics";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets the retention period for historical data.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(30);
}
