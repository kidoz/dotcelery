namespace DotCelery.Core.Storage;

/// <summary>
/// Atomic counters and sliding-window event counts.
/// </summary>
public interface ICounterStore
{
    /// <summary>
    /// Adds <paramref name="delta"/> to a counter, creating it at zero if it does not exist.
    /// </summary>
    /// <param name="key">The counter key.</param>
    /// <param name="delta">The amount to add; may be negative.</param>
    /// <param name="timeToLive">
    /// How long a newly created counter lives. The expiry of an existing counter is not changed.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The new value.</returns>
    ValueTask<long> IncrementAsync(
        string key,
        long delta = 1,
        TimeSpan? timeToLive = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets a counter value.
    /// </summary>
    /// <param name="key">The counter key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The value, or zero if the counter does not exist.</returns>
    ValueTask<long> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a counter.
    /// </summary>
    /// <param name="key">The counter key.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> if a counter was deleted.</returns>
    ValueTask<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records an event in a sliding window if fewer than <paramref name="limit"/> events were
    /// recorded within the last <paramref name="window"/>. Checking and recording are atomic.
    /// </summary>
    /// <param name="key">The window key.</param>
    /// <param name="limit">The maximum number of events within the window.</param>
    /// <param name="window">The window length.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether the event was recorded, and the resulting count.</returns>
    ValueTask<WindowResult> TryAddToWindowAsync(
        string key,
        int limit,
        TimeSpan window,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Gets the events recorded within the last <paramref name="window"/>.
    /// </summary>
    /// <param name="key">The window key.</param>
    /// <param name="window">The window length.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of events and the time of the oldest.</returns>
    ValueTask<WindowSnapshot> GetWindowAsync(
        string key,
        TimeSpan window,
        CancellationToken cancellationToken = default
    );
}

/// <summary>
/// The outcome of <see cref="ICounterStore.TryAddToWindowAsync"/>.
/// </summary>
/// <param name="Added">Whether the event was recorded.</param>
/// <param name="Count">The number of events in the window, including a recorded event.</param>
/// <param name="RetryAfter">
/// When the event was not recorded, how long until the oldest event leaves the window.
/// </param>
public readonly record struct WindowResult(bool Added, long Count, TimeSpan? RetryAfter);

/// <summary>
/// The events of a sliding window, from <see cref="ICounterStore.GetWindowAsync"/>.
/// </summary>
/// <param name="Count">The number of events within the window.</param>
/// <param name="OldestEvent">The time of the oldest event within the window, if any.</param>
public readonly record struct WindowSnapshot(long Count, DateTimeOffset? OldestEvent);
