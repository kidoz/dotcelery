namespace DotCelery.Backend.Mongo.Outbox;

/// <summary>
/// Options for <see cref="MongoOutboxStore"/>.
/// </summary>
public sealed class MongoOutboxStoreOptions
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
    /// Gets or sets the outbox collection name.
    /// </summary>
    public string CollectionName { get; set; } = "outbox";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;
}
