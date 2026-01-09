namespace DotCelery.Backend.Mongo.Batches;

/// <summary>
/// Options for <see cref="MongoBatchStore"/>.
/// </summary>
public sealed class MongoBatchStoreOptions
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
    /// Gets or sets the batches collection name.
    /// </summary>
    public string CollectionName { get; set; } = "batches";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;
}
