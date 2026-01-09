using DotCelery.Core.Resilience;
using DotCelery.Worker.Resilience;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Resilience;

/// <summary>
/// Tests for <see cref="CircuitBreaker"/>.
/// </summary>
public sealed class CircuitBreakerTests : IDisposable
{
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly CircuitBreakerOptions _options;
    private readonly CircuitBreaker _circuitBreaker;

    public CircuitBreakerTests()
    {
        _options = new CircuitBreakerOptions
        {
            FailureThreshold = 3,
            OpenDuration = TimeSpan.FromSeconds(30),
            SuccessThreshold = 2,
            FailureWindow = TimeSpan.FromMinutes(1),
        };

        _circuitBreaker = new CircuitBreaker(
            "test-circuit",
            _options,
            NullLogger<CircuitBreaker>.Instance,
            _timeProvider
        );
    }

    public void Dispose()
    {
        _circuitBreaker.Dispose();
    }

    [Fact]
    public void State_Initially_ReturnsClosed()
    {
        Assert.Equal(CircuitState.Closed, _circuitBreaker.State);
    }

    [Fact]
    public void IsAllowed_Initially_ReturnsTrue()
    {
        Assert.True(_circuitBreaker.IsAllowed);
    }

    [Fact]
    public void Name_ReturnsConfiguredName()
    {
        Assert.Equal("test-circuit", _circuitBreaker.Name);
    }

    [Fact]
    public void RecordSuccess_InClosedState_ResetsFailureCount()
    {
        _circuitBreaker.RecordFailure();
        _circuitBreaker.RecordFailure();

        Assert.Equal(2, _circuitBreaker.FailureCount);

        _circuitBreaker.RecordSuccess();

        Assert.Equal(0, _circuitBreaker.FailureCount);
    }

    [Fact]
    public void RecordFailure_BelowThreshold_StaysClosed()
    {
        _circuitBreaker.RecordFailure();
        _circuitBreaker.RecordFailure();

        Assert.Equal(CircuitState.Closed, _circuitBreaker.State);
        Assert.Equal(2, _circuitBreaker.FailureCount);
    }

    [Fact]
    public void RecordFailure_AtThreshold_TransitionsToOpen()
    {
        for (var i = 0; i < 3; i++)
        {
            _circuitBreaker.RecordFailure();
        }

        Assert.Equal(CircuitState.Open, _circuitBreaker.State);
        Assert.False(_circuitBreaker.IsAllowed);
        Assert.NotNull(_circuitBreaker.LastOpenedAt);
    }

    [Fact]
    public void RecordFailure_OutsideWindow_ResetsCount()
    {
        _circuitBreaker.RecordFailure();
        _circuitBreaker.RecordFailure();

        // Advance past failure window
        _timeProvider.Advance(TimeSpan.FromMinutes(2));

        // This should reset count
        _circuitBreaker.RecordFailure();

        Assert.Equal(1, _circuitBreaker.FailureCount);
        Assert.Equal(CircuitState.Closed, _circuitBreaker.State);
    }

    [Fact]
    public void Trip_ForcesOpenState()
    {
        _circuitBreaker.Trip();

        Assert.Equal(CircuitState.Open, _circuitBreaker.State);
        Assert.False(_circuitBreaker.IsAllowed);
    }

    [Fact]
    public void Reset_FromOpen_TransitionsToClosed()
    {
        // Open the circuit
        for (var i = 0; i < 3; i++)
        {
            _circuitBreaker.RecordFailure();
        }

        Assert.Equal(CircuitState.Open, _circuitBreaker.State);

        _circuitBreaker.Reset();

        Assert.Equal(CircuitState.Closed, _circuitBreaker.State);
        Assert.Equal(0, _circuitBreaker.FailureCount);
        Assert.True(_circuitBreaker.IsAllowed);
    }

    [Fact]
    public void StateChanged_RaisedOnTransitionToOpen()
    {
        var stateChanges = new List<CircuitStateChangedEventArgs>();
        _circuitBreaker.StateChanged += (_, args) => stateChanges.Add(args);

        for (var i = 0; i < 3; i++)
        {
            _circuitBreaker.RecordFailure();
        }

        Assert.Single(stateChanges);
        Assert.Equal(CircuitState.Closed, stateChanges[0].OldState);
        Assert.Equal(CircuitState.Open, stateChanges[0].NewState);
        Assert.Equal("test-circuit", stateChanges[0].CircuitName);
    }

    [Fact]
    public async Task ExecuteAsync_WhenOpen_ThrowsCircuitBreakerOpenException()
    {
        _circuitBreaker.Trip();

        await Assert.ThrowsAsync<CircuitBreakerOpenException>(() =>
            _circuitBreaker
                .ExecuteAsync<int>(_ => ValueTask.FromResult(42), CancellationToken.None)
                .AsTask()
        );
    }

    [Fact]
    public async Task ExecuteAsync_WhenClosed_ExecutesOperation()
    {
        var result = await _circuitBreaker.ExecuteAsync(
            _ => ValueTask.FromResult(42),
            CancellationToken.None
        );

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task ExecuteAsync_OnSuccess_RecordsSuccess()
    {
        _circuitBreaker.RecordFailure();
        Assert.Equal(1, _circuitBreaker.FailureCount);

        await _circuitBreaker.ExecuteAsync(_ => ValueTask.FromResult(42), CancellationToken.None);

        Assert.Equal(0, _circuitBreaker.FailureCount);
    }

    [Fact]
    public async Task ExecuteAsync_OnFailure_RecordsFailureAndRethrows()
    {
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _circuitBreaker
                .ExecuteAsync<int>(
                    _ => throw new InvalidOperationException("test"),
                    CancellationToken.None
                )
                .AsTask()
        );

        Assert.Equal("test", exception.Message);
        Assert.Equal(1, _circuitBreaker.FailureCount);
    }

    [Fact]
    public void RecordFailure_WithIgnoredException_DoesNotCountFailure()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 3,
            IgnoreExceptions = [typeof(OperationCanceledException)],
        };

        using var circuitBreaker = new CircuitBreaker(
            "test",
            options,
            NullLogger<CircuitBreaker>.Instance,
            _timeProvider
        );

        circuitBreaker.RecordFailure(new OperationCanceledException());
        circuitBreaker.RecordFailure(new OperationCanceledException());
        circuitBreaker.RecordFailure(new OperationCanceledException());

        Assert.Equal(CircuitState.Closed, circuitBreaker.State);
        Assert.Equal(0, circuitBreaker.FailureCount);
    }

    [Fact]
    public void RecordFailure_WithTripOnExceptions_OnlyCountsMatchingExceptions()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 3,
            TripOnExceptions = [typeof(TimeoutException)],
        };

        using var circuitBreaker = new CircuitBreaker(
            "test",
            options,
            NullLogger<CircuitBreaker>.Instance,
            _timeProvider
        );

        // These shouldn't count
        circuitBreaker.RecordFailure(new InvalidOperationException());
        circuitBreaker.RecordFailure(new ArgumentException());

        Assert.Equal(0, circuitBreaker.FailureCount);

        // These should count
        circuitBreaker.RecordFailure(new TimeoutException());
        circuitBreaker.RecordFailure(new TimeoutException());
        circuitBreaker.RecordFailure(new TimeoutException());

        Assert.Equal(CircuitState.Open, circuitBreaker.State);
    }

    [Fact]
    public async Task HalfOpen_OnSuccess_TransitionsToClosed()
    {
        var stateChanges = new List<CircuitStateChangedEventArgs>();

        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 3,
            OpenDuration = TimeSpan.FromMilliseconds(50),
            SuccessThreshold = 2,
        };

        using var circuitBreaker = new CircuitBreaker(
            "test",
            options,
            NullLogger<CircuitBreaker>.Instance
        );

        circuitBreaker.StateChanged += (_, args) => stateChanges.Add(args);

        // Trip the circuit
        for (var i = 0; i < 3; i++)
        {
            circuitBreaker.RecordFailure();
        }

        Assert.Equal(CircuitState.Open, circuitBreaker.State);

        // Wait for transition to half-open
        await Task.Delay(100, CancellationToken.None);

        Assert.Equal(CircuitState.HalfOpen, circuitBreaker.State);

        // Record enough successes to close
        circuitBreaker.RecordSuccess();
        circuitBreaker.RecordSuccess();

        Assert.Equal(CircuitState.Closed, circuitBreaker.State);
    }

    [Fact]
    public async Task HalfOpen_OnFailure_TransitionsBackToOpen()
    {
        var options = new CircuitBreakerOptions
        {
            FailureThreshold = 3,
            OpenDuration = TimeSpan.FromMilliseconds(50),
            SuccessThreshold = 2,
        };

        using var circuitBreaker = new CircuitBreaker(
            "test",
            options,
            NullLogger<CircuitBreaker>.Instance
        );

        // Trip the circuit
        for (var i = 0; i < 3; i++)
        {
            circuitBreaker.RecordFailure();
        }

        // Wait for half-open
        await Task.Delay(100, CancellationToken.None);

        Assert.Equal(CircuitState.HalfOpen, circuitBreaker.State);

        // Record a failure - should go back to open
        circuitBreaker.RecordFailure();

        Assert.Equal(CircuitState.Open, circuitBreaker.State);
    }
}
