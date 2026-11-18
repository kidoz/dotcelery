using DotCelery.Core.Exceptions;
using DotCelery.Core.TimeLimits;
using DotCelery.Worker.Execution;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.TimeLimits;

/// <summary>
/// Tests for <see cref="TimeLimitEnforcer"/>.
/// </summary>
public sealed class TimeLimitEnforcerTests
{
    private readonly FakeTimeProvider _clock = new();
    private readonly TimeLimitEnforcer _enforcer;

    public TimeLimitEnforcerTests() =>
        _enforcer = new(NullLogger<TimeLimitEnforcer>.Instance, _clock);

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task SoftTimeout_WaitsForExecutionAndObservesItsFinalOutcome(
        bool fails,
        bool callbackFails
    )
    {
        var cancelled = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var finish = new TaskCompletionSource<int>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var execution = _enforcer.ExecuteWithTimeLimitsAsync(
            "task",
            new TimeLimitPolicy(SoftLimit: TimeSpan.FromMilliseconds(20)),
            async ct =>
            {
                using var registration = ct.Register(() =>
                {
                    cancelled.TrySetResult();
                    if (callbackFails)
                        throw new InvalidOperationException("cancellation callback");
                });
                return await finish.Task;
            },
            CancellationToken.None
        );
        _clock.Advance(TimeSpan.FromMilliseconds(20));
        try
        {
            await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(execution.IsCompleted);
        }
        finally
        {
            if (fails)
                finish.TrySetException(new InvalidOperationException("late failure"));
            else
                finish.TrySetResult(42);
        }
        await Assert.ThrowsAsync<SoftTimeLimitExceededException>(() => execution);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteWithTimeLimitsAsync_CompletedTaskReturnsResult(bool withLimits)
    {
        var policy = withLimits
            ? new TimeLimitPolicy(
                SoftLimit: TimeSpan.FromSeconds(5),
                HardLimit: TimeSpan.FromSeconds(10)
            )
            : new TimeLimitPolicy();
        var result = await _enforcer.ExecuteWithTimeLimitsAsync(
            "task-1",
            policy,
            _ => Task.FromResult("result"),
            TestContext.Current.CancellationToken
        );
        Assert.Equal("result", result);
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_ExceedsHardLimit_ThrowsTimeoutException()
    {
        var limit = TimeSpan.FromSeconds(5);
        var execution = _enforcer.ExecuteWithTimeLimitsAsync(
            "task-1",
            new TimeLimitPolicy(HardLimit: limit),
            WaitForCancellationAsync,
            TestContext.Current.CancellationToken
        );
        _clock.Advance(limit);
        await Assert.ThrowsAsync<TimeoutException>(() => execution);
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_WithBothLimits_SoftTriggersFirst()
    {
        var softLimit = TimeSpan.FromSeconds(5);
        var policy = new TimeLimitPolicy(SoftLimit: softLimit, HardLimit: softLimit * 2);
        var execution = _enforcer.ExecuteWithTimeLimitsAsync(
            "task-1",
            policy,
            WaitForCancellationAsync,
            TestContext.Current.CancellationToken
        );
        _clock.Advance(softLimit);
        await Assert.ThrowsAsync<SoftTimeLimitExceededException>(() => execution);
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_RespectsExternalCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        var execution = _enforcer.ExecuteWithTimeLimitsAsync(
            "task-1",
            new TimeLimitPolicy(HardLimit: TimeSpan.FromSeconds(30)),
            WaitForCancellationAsync,
            cancellation.Token
        );
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => execution);
    }

    private static Task WaitForCancellationAsync(CancellationToken cancellationToken) =>
        Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);

    [Fact]
    public void TimeLimitPolicy_HasLimits_ReturnsFalseWhenNoLimits()
    {
        // Arrange
        var policy = new TimeLimitPolicy();

        // Assert
        Assert.False(policy.HasLimits);
    }

    [Fact]
    public void TimeLimitPolicy_HasLimits_ReturnsTrueWithSoftLimit()
    {
        // Arrange
        var policy = new TimeLimitPolicy(SoftLimit: TimeSpan.FromSeconds(30));

        // Assert
        Assert.True(policy.HasLimits);
    }

    [Fact]
    public void TimeLimitPolicy_HasLimits_ReturnsTrueWithHardLimit()
    {
        // Arrange
        var policy = new TimeLimitPolicy(HardLimit: TimeSpan.FromSeconds(60));

        // Assert
        Assert.True(policy.HasLimits);
    }
}
