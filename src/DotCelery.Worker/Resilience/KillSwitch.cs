using DotCelery.Core.Abstractions;
using DotCelery.Core.Resilience;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Resilience;

/// <summary>
/// Default implementation of <see cref="IKillSwitch"/> using a sliding window for failure tracking.
/// </summary>
public sealed class KillSwitch : IKillSwitch, IDisposable
{
    private readonly KillSwitchOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<KillSwitch> _logger;
    private readonly Lock _lock = new();
    private readonly Queue<(DateTimeOffset Timestamp, bool Success)> _window = new();
    private TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly CancellationTokenSource _disposeCts = new();

    private KillSwitchState _state = KillSwitchState.Ready;
    private DateTimeOffset? _lastTrippedAt;
    private Task? _restartTask;
    private long _restartVersion;
    private bool _disposed;
    private int _failureCount;
    private int _successCount;

    /// <summary>
    /// Initializes a new instance of the <see cref="KillSwitch"/> class.
    /// </summary>
    public KillSwitch(
        IOptions<KillSwitchOptions> options,
        ILogger<KillSwitch> logger,
        TimeProvider? timeProvider = null
    )
    {
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public KillSwitchState State
    {
        get
        {
            List<KillSwitchStateChangedEventArgs> changes = [];
            KillSwitchState state;
            lock (_lock)
            {
                if (
                    !_disposed
                    && _state == KillSwitchState.Tripped
                    && _timeProvider.GetUtcNow() - _lastTrippedAt >= _options.RestartTimeout
                )
                {
                    changes = RestartAndGetEvents();
                }
                state = _state;
            }
            foreach (var change in changes)
                RaiseStateChanged(change);
            return state;
        }
    }

    /// <inheritdoc />
    public bool IsPaused => State == KillSwitchState.Tripped;

    /// <inheritdoc />
    public DateTimeOffset? LastTrippedAt
    {
        get
        {
            lock (_lock)
            {
                return _lastTrippedAt;
            }
        }
    }

    /// <inheritdoc />
    public double CurrentFailureRate
    {
        get
        {
            lock (_lock)
            {
                PruneWindow();
                var total = _window.Count;
                return total == 0 ? 0 : (double)_failureCount / total;
            }
        }
    }

    /// <inheritdoc />
    public int TrackedMessageCount
    {
        get
        {
            lock (_lock)
            {
                PruneWindow();
                return _window.Count;
            }
        }
    }

    /// <inheritdoc />
    public event EventHandler<KillSwitchStateChangedEventArgs>? StateChanged;

    /// <inheritdoc />
    public void RecordSuccess()
    {
        KillSwitchStateChangedEventArgs? stateChangeEvent = null;

        lock (_lock)
        {
            if (_state == KillSwitchState.Tripped)
            {
                return;
            }

            var now = _timeProvider.GetUtcNow();
            _window.Enqueue((now, true));
            _successCount++;

            PruneWindow();
            stateChangeEvent = UpdateStateAndGetEvent(null);
        }

        // Raise event outside lock to prevent deadlock
        if (stateChangeEvent is not null)
        {
            RaiseStateChanged(stateChangeEvent);
        }
    }

    /// <inheritdoc />
    public void RecordFailure(Exception? exception = null)
    {
        KillSwitchStateChangedEventArgs? stateChangeEvent = null;

        lock (_lock)
        {
            if (_state == KillSwitchState.Tripped)
            {
                return;
            }

            // Check if this exception should be ignored
            if (exception is not null && ShouldIgnoreException(exception))
            {
                _logger.LogDebug(
                    "Exception {ExceptionType} ignored by kill switch",
                    exception.GetType().Name
                );
                return;
            }

            // Check if this exception type should trip the kill switch
            if (exception is not null && !ShouldTripOnException(exception))
            {
                return;
            }

            var now = _timeProvider.GetUtcNow();
            _window.Enqueue((now, false));
            _failureCount++;

            PruneWindow();
            stateChangeEvent = UpdateStateAndGetEvent(exception);
        }

        // Raise event outside lock to prevent deadlock
        if (stateChangeEvent is not null)
        {
            RaiseStateChanged(stateChangeEvent);
        }
    }

    /// <inheritdoc />
    public void Reset()
    {
        KillSwitchStateChangedEventArgs? stateChangeEvent = null;

        lock (_lock)
        {
            var oldState = _state;

            _window.Clear();
            _failureCount = 0;
            _successCount = 0;
            _state = KillSwitchState.Ready;
            _restartVersion++;

            _ready.TrySetResult();

            if (oldState != KillSwitchState.Ready)
            {
                _logger.LogInformation("Kill switch manually reset");

                stateChangeEvent = CreateStateChangedEvent(oldState, KillSwitchState.Ready, null);
            }
        }

        // Raise event outside lock to prevent deadlock
        if (stateChangeEvent is not null)
        {
            RaiseStateChanged(stateChangeEvent);
        }
    }

    /// <inheritdoc />
    public async ValueTask WaitUntilReadyAsync(CancellationToken cancellationToken = default)
    {
        // Fast path - not paused
        if (!IsPaused)
        {
            return;
        }

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _disposeCts.Token
        );

        while (IsPaused)
        {
            TimeSpan remaining;
            Task ready;
            lock (_lock)
            {
                if (_state != KillSwitchState.Tripped)
                    return;
                remaining =
                    _options.RestartTimeout - (_timeProvider.GetUtcNow() - _lastTrippedAt!.Value);
                ready = _ready.Task;
            }
            if (remaining <= TimeSpan.Zero)
                continue;
            try
            {
                await ready
                    .WaitAsync(remaining, _timeProvider, linkedCts.Token)
                    .ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Reading IsPaused completes the restart even if its timer failed.
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed)
                return;
            _disposed = true;
            _restartVersion++;
        }
        _disposeCts.Cancel();
        _disposeCts.Dispose();
    }

    private void PruneWindow()
    {
        var cutoff = _timeProvider.GetUtcNow() - _options.TrackingWindow;

        while (_window.Count > 0 && _window.Peek().Timestamp < cutoff)
        {
            var removed = _window.Dequeue();
            if (removed.Success)
            {
                _successCount--;
            }
            else
            {
                _failureCount--;
            }
        }
    }

    /// <summary>
    /// Updates state and returns an event to raise (if any) outside the lock.
    /// Must be called while holding _lock.
    /// </summary>
    private KillSwitchStateChangedEventArgs? UpdateStateAndGetEvent(Exception? lastException)
    {
        var total = _window.Count;
        var failureRate = total == 0 ? 0 : (double)_failureCount / total;

        var oldState = _state;

        switch (_state)
        {
            case KillSwitchState.Ready:
                if (total >= _options.ActivationThreshold)
                {
                    _state = KillSwitchState.Tracking;

                    _logger.LogDebug(
                        "Kill switch now tracking failures. Current rate: {Rate:P1}",
                        failureRate
                    );
                }
                break;

            case KillSwitchState.Tracking:
                if (total < _options.ActivationThreshold)
                {
                    _state = KillSwitchState.Ready;
                }
                else if (failureRate >= _options.TripThreshold)
                {
                    return TripKillSwitchAndGetEvent(failureRate, lastException);
                }
                break;

            case KillSwitchState.Restarting:
                // Still restarting - do nothing
                break;
        }

        if (oldState != _state && _state != KillSwitchState.Tripped)
        {
            // Don't raise event for Tripped state - TripKillSwitch handles that
            return CreateStateChangedEvent(oldState, _state, lastException);
        }

        return null;
    }

    /// <summary>
    /// Trips the kill switch and returns an event to raise outside the lock.
    /// Must be called while holding _lock.
    /// </summary>
    private KillSwitchStateChangedEventArgs TripKillSwitchAndGetEvent(
        double failureRate,
        Exception? exception
    )
    {
        _state = KillSwitchState.Tripped;
        _lastTrippedAt = _timeProvider.GetUtcNow();

        // Each pause owns its completion signal, so a waiter from an earlier pause cannot
        // release consumers after a reset followed by another trip.
        _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _logger.LogWarning(
            "Kill switch TRIPPED. Failure rate: {Rate:P1}, Threshold: {Threshold:P1}. "
                + "Consuming paused for {Timeout}",
            failureRate,
            _options.TripThreshold,
            _options.RestartTimeout
        );

        // Schedule automatic restart (fire and forget)
        _restartTask = ScheduleRestartAsync(++_restartVersion);

        return CreateStateChangedEvent(
            KillSwitchState.Tracking,
            KillSwitchState.Tripped,
            exception
        );
    }

    private async Task ScheduleRestartAsync(long version)
    {
        try
        {
            await Task.Delay(_options.RestartTimeout, _timeProvider, _disposeCts.Token)
                .ConfigureAwait(false);

            List<KillSwitchStateChangedEventArgs> eventsToRaise = [];
            lock (_lock)
            {
                if (!_disposed && version == _restartVersion && _state == KillSwitchState.Tripped)
                {
                    eventsToRaise = RestartAndGetEvents();
                }
            }

            // Raise events outside lock to prevent deadlock
            foreach (var evt in eventsToRaise)
            {
                RaiseStateChanged(evt);
            }
        }
        catch (OperationCanceledException)
        {
            // Dispose was called
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Kill switch timed restart failed; state checks will retry it");
        }
    }

    private List<KillSwitchStateChangedEventArgs> RestartAndGetEvents()
    {
        var restarting = CreateStateChangedEvent(_state, KillSwitchState.Restarting, null);
        _window.Clear();
        _failureCount = 0;
        _successCount = 0;
        _state = KillSwitchState.Ready;
        _ready.TrySetResult();
        _logger.LogInformation("Kill switch reset. Consumption resumed.");
        return
        [
            restarting,
            CreateStateChangedEvent(KillSwitchState.Restarting, KillSwitchState.Ready, null),
        ];
    }

    private KillSwitchStateChangedEventArgs CreateStateChangedEvent(
        KillSwitchState oldState,
        KillSwitchState newState,
        Exception? exception
    )
    {
        return new KillSwitchStateChangedEventArgs
        {
            OldState = oldState,
            NewState = newState,
            Timestamp = _timeProvider.GetUtcNow(),
            TriggeringException = exception,
            FailureCount = _failureCount,
            FailureRate = _window.Count == 0 ? 0 : (double)_failureCount / _window.Count,
        };
    }

    private bool ShouldIgnoreException(Exception exception)
    {
        if (_options.IgnoreExceptions.Count == 0)
        {
            return false;
        }

        var exceptionType = exception.GetType();
        return _options.IgnoreExceptions.Any(t => t.IsAssignableFrom(exceptionType));
    }

    private bool ShouldTripOnException(Exception exception)
    {
        if (_options.TripOnExceptions.Count == 0)
        {
            // No filter - all exceptions trip
            return true;
        }

        var exceptionType = exception.GetType();
        return _options.TripOnExceptions.Any(t => t.IsAssignableFrom(exceptionType));
    }

    private void RaiseStateChanged(KillSwitchStateChangedEventArgs eventArgs)
    {
        try
        {
            StateChanged?.Invoke(this, eventArgs);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error in kill switch state change handler");
        }
    }
}
