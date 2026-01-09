namespace DotCelery.Backend.Mongo.Execution;

/// <summary>
/// Options for <see cref="MongoTaskExecutionTracker"/>.
/// </summary>
public sealed class MongoTaskExecutionTrackerOptions
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
    public string CollectionName { get; set; } = "task_execution_tracking";

    /// <summary>
    /// Gets or sets whether to auto-create indexes.
    /// </summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets the default timeout for task execution tracking.
    /// </summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromHours(1);
}
