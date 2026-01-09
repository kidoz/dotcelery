namespace DotCelery.Worker.Services;

/// <summary>
/// Configuration options for <see cref="SignalQueueProcessor"/>.
/// </summary>
public sealed class SignalQueueProcessorOptions
{
    /// <summary>
    /// Gets or sets whether the signal queue processor is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets the polling interval for the signal queue.
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets the maximum batch size for dequeuing signals.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    /// <summary>
    /// Gets or sets whether to process signals in parallel within a batch.
    /// </summary>
    public bool ParallelProcessing { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum degree of parallelism for signal processing.
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = 4;
}
