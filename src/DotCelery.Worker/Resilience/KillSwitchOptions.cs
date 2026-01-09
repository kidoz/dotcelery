namespace DotCelery.Worker.Resilience;

/// <summary>
/// Configuration options for the kill switch.
/// </summary>
public sealed class KillSwitchOptions
{
    /// <summary>
    /// Gets or sets the minimum number of messages that must be processed
    /// before the failure rate is evaluated. Default: 10.
    /// </summary>
    public int ActivationThreshold
    {
        get;
        set =>
            field =
                value > 0
                    ? value
                    : throw new ArgumentOutOfRangeException(
                        nameof(value),
                        "ActivationThreshold must be positive."
                    );
    } = 10;

    /// <summary>
    /// Gets or sets the failure rate (0.0-1.0) that will trip the kill switch.
    /// Default: 0.15 (15% failure rate).
    /// </summary>
    public double TripThreshold
    {
        get;
        set =>
            field = value is >= 0.0 and <= 1.0
                ? value
                : throw new ArgumentOutOfRangeException(
                    nameof(value),
                    "TripThreshold must be between 0.0 and 1.0."
                );
    } = 0.15;

    /// <summary>
    /// Gets or sets the time window for calculating the failure rate.
    /// Messages outside this window are not counted. Default: 1 minute.
    /// </summary>
    public TimeSpan TrackingWindow
    {
        get;
        set =>
            field =
                value > TimeSpan.Zero
                    ? value
                    : throw new ArgumentOutOfRangeException(
                        nameof(value),
                        "TrackingWindow must be positive."
                    );
    } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets the time to wait before automatically restarting
    /// after the kill switch trips. Default: 1 minute.
    /// </summary>
    public TimeSpan RestartTimeout
    {
        get;
        set =>
            field =
                value > TimeSpan.Zero
                    ? value
                    : throw new ArgumentOutOfRangeException(
                        nameof(value),
                        "RestartTimeout must be positive."
                    );
    } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets the exception types that should trip the kill switch.
    /// If empty, all exceptions count. Default: empty (all exceptions).
    /// </summary>
    public IReadOnlyList<Type> TripOnExceptions
    {
        get;
        set => field = value ?? throw new ArgumentNullException(nameof(value));
    } = [];

    /// <summary>
    /// Gets or sets exception types to ignore when tracking failures.
    /// These exceptions will not count toward the failure rate.
    /// </summary>
    public IReadOnlyList<Type> IgnoreExceptions
    {
        get;
        set => field = value ?? throw new ArgumentNullException(nameof(value));
    } = [];
}
