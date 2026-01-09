namespace DotCelery.Backend.Mongo.DelayedMessageStore;

/// <summary>
/// Options for <see cref="MongoDelayedMessageStore"/>.
/// </summary>
public sealed class MongoDelayedMessageStoreOptions
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
    /// Gets or sets the collection name.
    /// </summary>
    public string CollectionName { get; set; } = "delayed_messages";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets the batch size for fetching due messages.
    /// </summary>
    public int BatchSize { get; set; } = 100;
}
