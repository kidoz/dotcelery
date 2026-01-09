namespace DotCelery.Backend.Mongo.DeadLetter;

/// <summary>
/// Options for <see cref="MongoDeadLetterStore"/>.
/// </summary>
public sealed class MongoDeadLetterStoreOptions
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
    public string CollectionName { get; set; } = "dead_letters";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets the default retention period.
    /// </summary>
    public TimeSpan DefaultRetention { get; set; } = TimeSpan.FromDays(7);
}
