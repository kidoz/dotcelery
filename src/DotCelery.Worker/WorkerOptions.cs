namespace DotCelery.Worker;

/// <summary>
/// Worker configuration options.
/// </summary>
public sealed class WorkerOptions
{
    /// <summary>
    /// Gets or sets the number of concurrent task processors.
    /// </summary>
    public int Concurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the queues to consume from.
    /// </summary>
    public IReadOnlyList<string> Queues { get; set; } = ["celery"];

    /// <summary>
    /// Gets or sets the worker name for identification.
    /// </summary>
    public string? WorkerName { get; set; }

    /// <summary>
    /// Gets or sets the prefetch count per worker.
    /// </summary>
    public int PrefetchCount { get; set; } = 4;

    /// <summary>
    /// Gets or sets the shutdown timeout.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    // ========== Delay Queue Settings ==========

    /// <summary>
    /// Gets or sets whether to use the delay queue for ETA/countdown tasks.
    /// When enabled, tasks with ETA are stored in a delay store instead of being requeued.
    /// </summary>
    public bool UseDelayQueue { get; set; }

    /// <summary>
    /// Gets or sets the poll interval for checking delayed messages.
    /// The dispatcher will adaptively adjust based on next delivery time.
    /// </summary>
    public TimeSpan DelayedMessagePollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the retry interval for failed delayed message dispatches.
    /// </summary>
    public TimeSpan DelayedMessageRetryInterval { get; set; } = TimeSpan.FromSeconds(5);

    // ========== Revocation Settings ==========

    /// <summary>
    /// Gets or sets whether to enable task revocation checking.
    /// When enabled, tasks are checked against the revocation store before execution.
    /// </summary>
    public bool EnableRevocation { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to check revocation status before processing each task.
    /// Disable this for better performance if revocation is not needed.
    /// </summary>
    public bool CheckRevocationBeforeExecution { get; set; } = true;

    // ========== Rate Limiting Settings ==========

    /// <summary>
    /// Gets or sets whether to enable rate limiting for tasks.
    /// When enabled, tasks with RateLimitAttribute are rate-limited before execution.
    /// </summary>
    public bool EnableRateLimiting { get; set; } = true;

    /// <summary>
    /// Gets or sets the default requeue delay when a task is rate-limited.
    /// If null, uses the RetryAfter value from the rate limiter.
    /// </summary>
    public TimeSpan? RateLimitRequeueDelay { get; set; }

    /// <summary>
    /// Gets or sets whether to requeue rate-limited tasks to the delay queue.
    /// When false, rate-limited tasks are immediately requeued to the broker.
    /// </summary>
    public bool RequeueRateLimitedToDelayQueue { get; set; } = true;

    // ========== Graceful Shutdown Settings ==========

    /// <summary>
    /// Gets or sets whether to wait for in-flight tasks during shutdown.
    /// When enabled, the worker will wait for active tasks to complete before stopping.
    /// </summary>
    public bool EnableGracefulShutdown { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to nack unfinished messages for redelivery on forced shutdown.
    /// When enabled, messages for tasks that didn't complete within the shutdown timeout
    /// will be rejected with requeue=true so they can be processed by another worker.
    /// </summary>
    public bool NackOnForcedShutdown { get; set; } = true;

    /// <summary>
    /// Gets or sets the interval to log shutdown progress.
    /// During graceful shutdown, progress will be logged at this interval.
    /// </summary>
    public TimeSpan ShutdownProgressInterval { get; set; } = TimeSpan.FromSeconds(5);
}
