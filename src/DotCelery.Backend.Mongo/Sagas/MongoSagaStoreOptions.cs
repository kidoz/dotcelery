namespace DotCelery.Backend.Mongo.Sagas;

/// <summary>
/// Options for <see cref="MongoSagaStore"/>.
/// </summary>
public sealed class MongoSagaStoreOptions
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
    /// Gets or sets the sagas collection name.
    /// </summary>
    public string CollectionName { get; set; } = "sagas";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;
}
