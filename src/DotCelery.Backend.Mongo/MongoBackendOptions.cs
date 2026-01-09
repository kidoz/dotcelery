namespace DotCelery.Backend.Mongo;

/// <summary>
/// Configuration options for the MongoDB result backend.
/// </summary>
public sealed class MongoBackendOptions
{
    /// <summary>
    /// Gets or sets the MongoDB connection string.
    /// Example: "mongodb://localhost:27017"
    /// </summary>
    public string ConnectionString { get; set; } = "mongodb://localhost:27017";

    /// <summary>
    /// Gets or sets the database name.
    /// </summary>
    public string DatabaseName { get; set; } = "celery";

    /// <summary>
    /// Gets or sets the collection name for task results.
    /// </summary>
    public string CollectionName { get; set; } = "celery_task_results";

    /// <summary>
    /// Gets or sets the default result expiry time.
    /// Results older than this will be automatically deleted by MongoDB TTL index.
    /// </summary>
    public TimeSpan DefaultExpiry { get; set; } = TimeSpan.FromDays(1);

    /// <summary>
    /// Gets or sets the polling interval when waiting for results.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets whether to use change streams for result notifications.
    /// Requires MongoDB replica set or sharded cluster.
    /// </summary>
    public bool UseChangeStreams { get; set; }

    /// <summary>
    /// Gets or sets whether to automatically create indexes on startup.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets the server selection timeout.
    /// </summary>
    public TimeSpan ServerSelectionTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the connect timeout.
    /// </summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
