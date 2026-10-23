using DotCelery.Core.Abstractions;
using DotCelery.Core.Resilience;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Resilience;

/// <summary>
/// Default implementation of <see cref="ICircuitBreaker"/> using a state machine.
/// </summary>
public sealed class CircuitBreaker : ICircuitBreaker, IDisposable
{
    private readonly CircuitBreakerOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _disposeCts = new();

    private CircuitState _state = CircuitState.Closed;
    private int _failureCount;
    private int _successCount;
    private DateTimeOffset? _lastOpenedAt;
    private DateTimeOffset? _lastFailureAt;
    private Task? _halfOpenTransitionTask;
    private long _transitionVersion;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="CircuitBreaker"/> class.
    /// </summary>
    public CircuitBreaker(
        string name,
        CircuitBreakerOptions options,
        ILogger logger,
        TimeProvider? timeProvider = null
    )
    {
        Name = name;
        _options = options;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public CircuitState State
    {
        get
        {
            CircuitStateChangedEventArgs? change = null;
            CircuitState state;
            lock (_lock)
            {
                if (
                    !_disposed
                    && _state == CircuitState.Open
                    && _timeProvider.GetUtcNow() - _lastOpenedAt >= _options.OpenDuration
                )
                {
                    change = TransitionToAndGetEvent(CircuitState.HalfOpen, null);
                }
                state = _state;
            }
            if (change is not null)
            {
                RaiseStateChanged(change);
            }
            return state;
        }
    }

    /// <inheritdoc />
    public bool IsAllowed => State != CircuitState.Open;

    /// <inheritdoc />
    public DateTimeOffset? LastOpenedAt
    {
        get
        {
            lock (_lock)
            {
                return _lastOpenedAt;
            }
        }
    }

    /// <inheritdoc />
    public int FailureCount
    {
        get
        {
            lock (_lock)
            {
                return _failureCount;
            }
        }
    }

    /// <inheritdoc />
    public int SuccessCount
    {
        get
        {
            lock (_lock)
            {
                return _successCount;
            }
        }
    }

    /// <inheritdoc />
    public event EventHandler<CircuitStateChangedEventArgs>? StateChanged;

    /// <inheritdoc />
    public void RecordSuccess()
    {
        CircuitStateChangedEventArgs? stateChangeEvent = null;

        lock (_lock)
        {
            switch (_state)
            {
                case CircuitState.Closed:
                    // Reset failure count on success
                    _failureCount = 0;
                    break;

                case CircuitState.HalfOpen:
                    _successCount++;
                    _logger.LogDebug(
                        "Circuit '{Name}' half-open success {Count}/{Threshold}",
                        Name,
                        _successCount,
                        _options.SuccessThreshold
                    );

                    if (_successCount >= _options.SuccessThreshold)
                    {
                        stateChangeEvent = TransitionToAndGetEvent(CircuitState.Closed, null);
                    }
                    break;

                case CircuitState.Open:
                    // Shouldn't happen, but ignore
                    break;
            }
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
        CircuitStateChangedEventArgs? stateChangeEvent = null;

        lock (_lock)
        {
            // Check if this exception should be ignored
            if (exception is not null && ShouldIgnoreException(exception))
            {
                return;
            }

            // Check if this exception type should trip the circuit
            if (exception is not null && !ShouldTripOnException(exception))
            {
                return;
            }

            var now = _timeProvider.GetUtcNow();

            switch (_state)
            {
                case CircuitState.Closed:
                    // Check if previous failure is outside window
                    if (
                        _lastFailureAt.HasValue
                        && now - _lastFailureAt.Value > _options.FailureWindow
                    )
                    {
                        _failureCount = 0;
                    }

                    _failureCount++;
                    _lastFailureAt = now;

                    _logger.LogDebug(
                        "Circuit '{Name}' failure {Count}/{Threshold}",
                        Name,
                        _failureCount,
                        _options.FailureThreshold
                    );

                    if (_failureCount >= _options.FailureThreshold)
                    {
                        stateChangeEvent = TransitionToAndGetEvent(CircuitState.Open, exception);
                    }
                    break;

                case CircuitState.HalfOpen:
                    // Any failure in half-open transitions back to open
                    _logger.LogWarning(
                        "Circuit '{Name}' failed in half-open state, reopening",
                        Name
                    );
                    stateChangeEvent = TransitionToAndGetEvent(CircuitState.Open, exception);
                    break;

                case CircuitState.Open:
                    // Already open, ignore
                    break;
            }
        }

        // Raise event outside lock to prevent deadlock
        if (stateChangeEvent is not null)
        {
            RaiseStateChanged(stateChangeEvent);
        }
    }

    /// <inheritdoc />
    public async ValueTask<T> ExecuteAsync<T>(
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken = default
    )
    {
        lock (_lock)
        {
            if (_state == CircuitState.Open)
            {
                throw new CircuitBreakerOpenException(
                    Name,
                    _options.OpenDuration - (_timeProvider.GetUtcNow() - _lastOpenedAt!.Value)
                );
            }
        }

        try
        {
            var result = await operation(cancellationToken).ConfigureAwait(false);
            RecordSuccess();
            return result;
        }
        catch (Exception ex) when (ex is not CircuitBreakerOpenException)
        {
            RecordFailure(ex);
            throw;
        }
    }

    /// <inheritdoc />
    public void Trip()
    {
        CircuitStateChangedEventArgs? stateChangeEvent = null;

        lock (_lock)
        {
            if (_state != CircuitState.Open)
            {
                stateChangeEvent = TransitionToAndGetEvent(CircuitState.Open, null);
            }
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
        CircuitStateChangedEventArgs? stateChangeEvent = null;

        lock (_lock)
        {
            var oldState = _state;
            _state = CircuitState.Closed;
            _transitionVersion++;
            _failureCount = 0;
            _successCount = 0;

            if (oldState != CircuitState.Closed)
            {
                _logger.LogInformation("Circuit '{Name}' manually reset to closed", Name);
                stateChangeEvent = CreateStateChangedEvent(oldState, CircuitState.Closed, null);
            }
        }

        // Raise event outside lock to prevent deadlock
        if (stateChangeEvent is not null)
        {
            RaiseStateChanged(stateChangeEvent);
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
            _transitionVersion++;
        }
        _disposeCts.Cancel();
        _disposeCts.Dispose();
    }

    /// <summary>
    /// Transitions to a new state and returns an event to raise outside the lock.
    /// Must be called while holding _lock.
    /// </summary>
    private CircuitStateChangedEventArgs TransitionToAndGetEvent(
        CircuitState newState,
        Exception? exception
    )
    {
        var oldState = _state;
        _state = newState;

        switch (newState)
        {
            case CircuitState.Open:
                _lastOpenedAt = _timeProvider.GetUtcNow();
                _successCount = 0;

                _logger.LogWarning(
                    "Circuit '{Name}' OPENED after {Failures} failures. Will retry in {Duration}",
                    Name,
                    _failureCount,
                    _options.OpenDuration
                );

                // Schedule transition to half-open
                _halfOpenTransitionTask = ScheduleHalfOpenAsync(++_transitionVersion);
                break;

            case CircuitState.HalfOpen:
                _successCount = 0;
                _logger.LogInformation("Circuit '{Name}' transitioning to HALF-OPEN", Name);
                break;

            case CircuitState.Closed:
                _failureCount = 0;
                _successCount = 0;
                _logger.LogInformation("Circuit '{Name}' CLOSED", Name);
                break;
        }

        return CreateStateChangedEvent(oldState, newState, exception);
    }

    private async Task ScheduleHalfOpenAsync(long version)
    {
        try
        {
            await Task.Delay(_options.OpenDuration, _timeProvider, _disposeCts.Token)
                .ConfigureAwait(false);

            CircuitStateChangedEventArgs? stateChangeEvent = null;

            lock (_lock)
            {
                if (!_disposed && version == _transitionVersion && _state == CircuitState.Open)
                {
                    stateChangeEvent = TransitionToAndGetEvent(CircuitState.HalfOpen, null);
                }
            }

            // Raise event outside lock to prevent deadlock
            if (stateChangeEvent is not null)
            {
                RaiseStateChanged(stateChangeEvent);
            }
        }
        catch (OperationCanceledException)
        {
            // Dispose was called
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Circuit '{Name}' timed transition failed; state checks will retry it",
                Name
            );
        }
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

    private CircuitStateChangedEventArgs CreateStateChangedEvent(
        CircuitState oldState,
        CircuitState newState,
        Exception? exception
    )
    {
        return new CircuitStateChangedEventArgs
        {
            CircuitName = Name,
            OldState = oldState,
            NewState = newState,
            Timestamp = _timeProvider.GetUtcNow(),
            TriggeringException = exception,
            FailureCount = _failureCount,
        };
    }

    private void RaiseStateChanged(CircuitStateChangedEventArgs eventArgs)
    {
        try
        {
            StateChanged?.Invoke(this, eventArgs);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error in circuit breaker state change handler");
        }
    }
}
