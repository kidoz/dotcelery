namespace DotCelery.Backend.Mongo.Storage;

/// <summary>
/// Options for the MongoDB storage primitives.
/// </summary>
public sealed class MongoStorageOptions
{
    /// <summary>
    /// The connection string used when none is configured. Suitable for local development only.
    /// </summary>
    public const string DevelopmentDefaultConnectionString = "mongodb://localhost:27017";

    /// <summary>
    /// Gets or sets the MongoDB connection string. Default is <c>mongodb://localhost:27017</c>.
    /// </summary>
    public string ConnectionString { get; set; } = DevelopmentDefaultConnectionString;

    /// <summary>
    /// Gets or sets the database name. Default is <c>dotcelery</c>.
    /// </summary>
    public string DatabaseName { get; set; } = "dotcelery";

    /// <summary>
    /// Gets or sets the prefix of every collection the storage uses. Default is <c>dotcelery_</c>.
    /// </summary>
    public string CollectionPrefix { get; set; } = "dotcelery_";

    /// <summary>
    /// Gets or sets whether notifications are sent through a capped collection. Turn this off
    /// where capped collections are not available; stores then poll. Default is <c>true</c>.
    /// </summary>
    public bool UseNotifications { get; set; } = true;

    /// <summary>
    /// Gets or sets the size of the capped collection that carries notifications, in bytes.
    /// Default is 1 MiB.
    /// </summary>
    public long NotificationCollectionSize { get; set; } = 1024 * 1024;

    /// <summary>
    /// Gets or sets how long to wait for a server. Default is 30 seconds.
    /// </summary>
    public TimeSpan ServerSelectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets how long to wait for a connection. Default is 10 seconds.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
