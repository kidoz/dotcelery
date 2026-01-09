namespace DotCelery.Backend.Mongo.Signals;

/// <summary>
/// Options for <see cref="MongoSignalStore"/>.
/// </summary>
public sealed class MongoSignalStoreOptions
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
    /// Gets or sets the signals collection name.
    /// </summary>
    public string CollectionName { get; set; } = "signals";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets the visibility timeout for processing messages.
    /// Messages will become visible again if not acknowledged within this time.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
