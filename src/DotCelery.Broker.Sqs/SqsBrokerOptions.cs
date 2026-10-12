namespace DotCelery.Broker.Sqs;

/// <summary>
/// Configuration options for the Amazon SQS broker.
/// </summary>
public sealed class SqsBrokerOptions
{
    /// <summary>
    /// Gets or sets the AWS region. Default is <c>us-east-1</c>.
    /// </summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// Gets or sets the service URL to send requests to, such as
    /// <c>http://localhost:4566</c> for LocalStack. When not set, the AWS endpoints are used.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>
    /// Gets or sets the access key. When not set, the AWS credential chain is used
    /// (environment, profile, instance role).
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// Gets or sets the secret key. Only used with <see cref="AccessKey"/>.
    /// </summary>
    public string? SecretKey { get; set; }

    /// <summary>
    /// Gets or sets whether queues are created when they do not exist. The created queue uses
    /// <see cref="VisibilityTimeout"/> and <see cref="MessageRetention"/>, and no redrive
    /// policy: configure dead-lettering on the queue itself. Default is true.
    /// </summary>
    public bool AutoCreateQueues { get; set; } = true;

    /// <summary>
    /// Gets or sets how long a delivered message stays hidden while its consumer processes it.
    /// A message whose consumer stopped is delivered again after this. Default is 5 minutes.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets whether the visibility of a message being processed is renewed, so a task
    /// that runs longer than <see cref="VisibilityTimeout"/> is not delivered to another
    /// consumer and run twice. Default is true.
    /// </summary>
    public bool RenewVisibilityWhileProcessing { get; set; } = true;

    /// <summary>
    /// Gets or sets how long a receive call waits for messages, between 0 and 20 seconds.
    /// Default is 20 seconds.
    /// </summary>
    public TimeSpan WaitTime { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Gets or sets how many messages one receive call asks for, between 1 and 10.
    /// Default is 10.
    /// </summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    /// Gets or sets how long SQS keeps messages. Default is 4 days, which is SQS's default.
    /// </summary>
    public TimeSpan MessageRetention { get; set; } = TimeSpan.FromDays(4);

    /// <summary>
    /// Gets or sets the largest message the broker publishes. SQS does not accept more than
    /// 256 KiB. Set to 0 to disable the check. Default is 256 KiB.
    /// </summary>
    public int MaxMessageSizeBytes { get; set; } = 262_144;
}
