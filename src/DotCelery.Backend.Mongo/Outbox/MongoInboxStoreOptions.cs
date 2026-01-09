namespace DotCelery.Backend.Mongo.Outbox;

/// <summary>
/// Options for <see cref="MongoInboxStore"/>.
/// </summary>
public sealed class MongoInboxStoreOptions
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
    /// Gets or sets the inbox collection name.
    /// </summary>
    public string CollectionName { get; set; } = "inbox";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;
}
