namespace DotCelery.Core.Sagas;

/// <summary>
/// Configuration options for saga orchestration.
/// </summary>
public sealed class SagaOptions
{
    /// <summary>
    /// Gets or sets whether sagas are enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to automatically compensate on step failure.
    /// If false, the saga will remain in Failed state until manually compensated.
    /// </summary>
    public bool AutoCompensateOnFailure { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum compensation retry attempts per step.
    /// </summary>
    public int MaxCompensationRetries { get; set; } = 3;

    /// <summary>
    /// Gets or sets the delay between compensation retries.
    /// </summary>
    public TimeSpan CompensationRetryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the timeout for waiting on step completion.
    /// </summary>
    public TimeSpan StepTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Gets or sets the retention period for completed sagas.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Gets or sets whether to dispatch signals for saga state changes.
    /// </summary>
    public bool DispatchSignals { get; set; } = true;
}
