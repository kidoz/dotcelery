namespace DotCelery.Broker.RabbitMQ;

/// <summary>
/// Configuration options for the RabbitMQ broker.
/// </summary>
public sealed class RabbitMQBrokerOptions
{
    /// <summary>
    /// Gets or sets the RabbitMQ connection string (AMQP URI).
    /// Example: "amqp://guest:guest@localhost:5672/"
    /// </summary>
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";

    /// <summary>
    /// Gets or sets the client-provided connection name for identification.
    /// </summary>
    public string? ConnectionName { get; set; } = "DotCelery";

    /// <summary>
    /// Gets or sets the prefetch count (number of unacknowledged messages per consumer).
    /// </summary>
    public ushort PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Gets or sets whether to declare queues automatically.
    /// </summary>
    public bool AutoDeclareQueues { get; set; } = true;

    /// <summary>
    /// Gets or sets whether queues should be durable (survive broker restart).
    /// </summary>
    public bool DurableQueues { get; set; } = true;

    /// <summary>
    /// Gets or sets the default exchange name. Empty string uses the default exchange.
    /// </summary>
    public string Exchange { get; set; } = "";

    /// <summary>
    /// Gets or sets the connection retry count.
    /// </summary>
    public int ConnectionRetryCount { get; set; } = 5;

    /// <summary>
    /// Gets or sets the delay between connection retries.
    /// </summary>
    public TimeSpan ConnectionRetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the heartbeat interval.
    /// </summary>
    public TimeSpan Heartbeat { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Gets or sets the maximum message size in bytes.
    /// Messages larger than this will be rejected.
    /// Default is 10 MB. Set to 0 to disable size checking.
    /// </summary>
    public int MaxMessageSizeBytes { get; set; } = 10 * 1024 * 1024;
}
