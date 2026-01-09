using DotCelery.Core.Resilience;
using DotCelery.Worker.Resilience;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Resilience;

/// <summary>
/// Tests for <see cref="KillSwitch"/>.
/// </summary>
public sealed class KillSwitchTests : IDisposable
{
    private readonly FakeTimeProvider _timeProvider = new();
    private readonly KillSwitchOptions _options;
    private readonly KillSwitch _killSwitch;

    public KillSwitchTests()
    {
        _options = new KillSwitchOptions
        {
            ActivationThreshold = 5,
            TripThreshold = 0.20, // 20% failure rate
            TrackingWindow = TimeSpan.FromMinutes(1),
            RestartTimeout = TimeSpan.FromMinutes(1),
        };

        _killSwitch = new KillSwitch(
            Options.Create(_options),
            NullLogger<KillSwitch>.Instance,
            _timeProvider
        );
    }

    public void Dispose()
    {
        _killSwitch.Dispose();
    }

    [Fact]
    public void State_Initially_ReturnsReady()
    {
        Assert.Equal(KillSwitchState.Ready, _killSwitch.State);
    }

    [Fact]
    public void IsPaused_Initially_ReturnsFalse()
    {
        Assert.False(_killSwitch.IsPaused);
    }

    [Fact]
    public void CurrentFailureRate_Initially_ReturnsZero()
    {
        Assert.Equal(0, _killSwitch.CurrentFailureRate);
    }

    [Fact]
    public void RecordSuccess_IncrementsTrackedMessageCount()
    {
        _killSwitch.RecordSuccess();
        _killSwitch.RecordSuccess();

        Assert.Equal(2, _killSwitch.TrackedMessageCount);
    }

    [Fact]
    public void RecordFailure_IncrementsTrackedMessageCount()
    {
        _killSwitch.RecordFailure();

        Assert.Equal(1, _killSwitch.TrackedMessageCount);
    }

    [Fact]
    public void RecordFailure_UpdatesFailureRate()
    {
        _killSwitch.RecordSuccess();
        _killSwitch.RecordSuccess();
        _killSwitch.RecordFailure();

        // 1 failure out of 3 = 33.3%
        Assert.True(_killSwitch.CurrentFailureRate > 0.33);
    }

    [Fact]
    public void State_BelowActivationThreshold_StaysReady()
    {
        // Record 4 messages (below threshold of 5)
        _killSwitch.RecordSuccess();
        _killSwitch.RecordSuccess();
        _killSwitch.RecordSuccess();
        _killSwitch.RecordFailure();

        Assert.Equal(KillSwitchState.Ready, _killSwitch.State);
    }

    [Fact]
    public void State_AtActivationThreshold_TransitionsToTracking()
    {
        // Record 5 messages (at threshold)
        for (var i = 0; i < 5; i++)
        {
            _killSwitch.RecordSuccess();
        }

        Assert.Equal(KillSwitchState.Tracking, _killSwitch.State);
    }

    [Fact]
    public void State_ExceedsTripThreshold_TripsKillSwitch()
    {
        // Record enough failures to trip: need 20% failure rate with 5+ messages
        // 4 success + 2 failures = 33% failure rate
        for (var i = 0; i < 4; i++)
        {
            _killSwitch.RecordSuccess();
        }

        _killSwitch.RecordFailure();
        _killSwitch.RecordFailure();

        Assert.Equal(KillSwitchState.Tripped, _killSwitch.State);
        Assert.True(_killSwitch.IsPaused);
        Assert.NotNull(_killSwitch.LastTrippedAt);
    }

    [Fact]
    public void State_BelowTripThreshold_StaysTracking()
    {
        // Record 9 successes + 1 failure = 10% failure rate (below 20%)
        for (var i = 0; i < 9; i++)
        {
            _killSwitch.RecordSuccess();
        }

        _killSwitch.RecordFailure();

        Assert.Equal(KillSwitchState.Tracking, _killSwitch.State);
        Assert.False(_killSwitch.IsPaused);
    }

    [Fact]
    public void Reset_ResetsStateToReady()
    {
        // Trip the kill switch
        TripKillSwitch();

        // Reset
        _killSwitch.Reset();

        Assert.Equal(KillSwitchState.Ready, _killSwitch.State);
        Assert.False(_killSwitch.IsPaused);
        Assert.Equal(0, _killSwitch.TrackedMessageCount);
    }

    [Fact]
    public async Task WaitUntilReadyAsync_WhenNotPaused_CompletesImmediately()
    {
        await _killSwitch.WaitUntilReadyAsync();
        // Should not hang
    }

    [Fact]
    public void StateChanged_RaisedOnStateTransition()
    {
        var stateChanges = new List<KillSwitchStateChangedEventArgs>();
        _killSwitch.StateChanged += (_, args) => stateChanges.Add(args);

        // Move to tracking
        for (var i = 0; i < 5; i++)
        {
            _killSwitch.RecordSuccess();
        }

        Assert.Single(stateChanges);
        Assert.Equal(KillSwitchState.Ready, stateChanges[0].OldState);
        Assert.Equal(KillSwitchState.Tracking, stateChanges[0].NewState);
    }

    [Fact]
    public void StateChanged_RaisedOnTrip()
    {
        var stateChanges = new List<KillSwitchStateChangedEventArgs>();
        _killSwitch.StateChanged += (_, args) => stateChanges.Add(args);

        TripKillSwitch();

        // Should have two transitions: Ready -> Tracking -> Tripped
        Assert.Equal(2, stateChanges.Count);
        Assert.Equal(KillSwitchState.Tripped, stateChanges[1].NewState);
    }

    [Fact]
    public void RecordFailure_WithIgnoredException_DoesNotCountFailure()
    {
        var options = new KillSwitchOptions
        {
            ActivationThreshold = 5,
            TripThreshold = 0.20,
            IgnoreExceptions = [typeof(OperationCanceledException)],
        };

        using var killSwitch = new KillSwitch(
            Options.Create(options),
            NullLogger<KillSwitch>.Instance,
            _timeProvider
        );

        for (var i = 0; i < 4; i++)
        {
            killSwitch.RecordSuccess();
        }

        killSwitch.RecordFailure(new OperationCanceledException());
        killSwitch.RecordFailure(new OperationCanceledException());

        // Ignored exceptions shouldn't trip
        Assert.NotEqual(KillSwitchState.Tripped, killSwitch.State);
    }

    [Fact]
    public void WindowPruning_RemovesOldMessages()
    {
        // Record some messages
        for (var i = 0; i < 5; i++)
        {
            _killSwitch.RecordSuccess();
        }

        Assert.Equal(5, _killSwitch.TrackedMessageCount);

        // Advance time past the window
        _timeProvider.Advance(TimeSpan.FromMinutes(2));

        // Record a new message (triggers pruning)
        _killSwitch.RecordSuccess();

        // Old messages should be pruned, only the new one remains
        Assert.Equal(1, _killSwitch.TrackedMessageCount);
    }

    private void TripKillSwitch()
    {
        // 4 successes + 2 failures = 33% failure rate, above 20% threshold
        for (var i = 0; i < 4; i++)
        {
            _killSwitch.RecordSuccess();
        }

        _killSwitch.RecordFailure();
        _killSwitch.RecordFailure();
    }
}
