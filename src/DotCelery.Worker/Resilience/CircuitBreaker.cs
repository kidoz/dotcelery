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
            lock (_lock)
            {
                return _state;
            }
        }
    }

    /// <inheritdoc />
    public bool IsAllowed
    {
        get
        {
            lock (_lock)
            {
                return _state != CircuitState.Open;
            }
        }
    }

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
                        TransitionTo(CircuitState.Closed, null);
                    }
                    break;

                case CircuitState.Open:
                    // Shouldn't happen, but ignore
                    break;
            }
        }
    }

    /// <inheritdoc />
    public void RecordFailure(Exception? exception = null)
    {
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
                        TransitionTo(CircuitState.Open, exception);
                    }
                    break;

                case CircuitState.HalfOpen:
                    // Any failure in half-open transitions back to open
                    _logger.LogWarning(
                        "Circuit '{Name}' failed in half-open state, reopening",
                        Name
                    );
                    TransitionTo(CircuitState.Open, exception);
                    break;

                case CircuitState.Open:
                    // Already open, ignore
                    break;
            }
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
        lock (_lock)
        {
            if (_state != CircuitState.Open)
            {
                TransitionTo(CircuitState.Open, null);
            }
        }
    }

    /// <inheritdoc />
    public void Reset()
    {
        lock (_lock)
        {
            var oldState = _state;
            _state = CircuitState.Closed;
            _failureCount = 0;
            _successCount = 0;

            if (oldState != CircuitState.Closed)
            {
                _logger.LogInformation("Circuit '{Name}' manually reset to closed", Name);
                RaiseStateChanged(oldState, CircuitState.Closed, null);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _disposeCts.Cancel();
        _disposeCts.Dispose();
    }

    private void TransitionTo(CircuitState newState, Exception? exception)
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
                _halfOpenTransitionTask = ScheduleHalfOpenAsync();
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

        RaiseStateChanged(oldState, newState, exception);
    }

    private async Task ScheduleHalfOpenAsync()
    {
        try
        {
            await Task.Delay(_options.OpenDuration, _disposeCts.Token).ConfigureAwait(false);

            lock (_lock)
            {
                if (_state == CircuitState.Open)
                {
                    TransitionTo(CircuitState.HalfOpen, null);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Dispose was called
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

    private void RaiseStateChanged(
        CircuitState oldState,
        CircuitState newState,
        Exception? exception
    )
    {
        try
        {
            StateChanged?.Invoke(
                this,
                new CircuitStateChangedEventArgs
                {
                    CircuitName = Name,
                    OldState = oldState,
                    NewState = newState,
                    Timestamp = _timeProvider.GetUtcNow(),
                    TriggeringException = exception,
                    FailureCount = _failureCount,
                }
            );
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error in circuit breaker state change handler");
        }
    }
}
