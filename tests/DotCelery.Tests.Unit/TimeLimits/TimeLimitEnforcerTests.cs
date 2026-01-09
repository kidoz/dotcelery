using DotCelery.Core.Exceptions;
using DotCelery.Core.TimeLimits;
using DotCelery.Worker.Execution;
using Microsoft.Extensions.Logging.Abstractions;

namespace DotCelery.Tests.Unit.TimeLimits;

/// <summary>
/// Tests for <see cref="TimeLimitEnforcer"/>.
/// </summary>
public sealed class TimeLimitEnforcerTests
{
    private readonly TimeLimitEnforcer _enforcer = new(NullLogger<TimeLimitEnforcer>.Instance);

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_WithNoLimits_ExecutesNormally()
    {
        // Arrange
        var policy = new TimeLimitPolicy();
        var executed = false;

        // Act
        await _enforcer.ExecuteWithTimeLimitsAsync(
            "task-1",
            policy,
            async _ =>
            {
                executed = true;
                await Task.Delay(10, CancellationToken.None);
                return "result";
            },
            CancellationToken.None
        );

        // Assert
        Assert.True(executed);
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_WithinSoftLimit_Succeeds()
    {
        // Arrange
        var policy = new TimeLimitPolicy(SoftLimit: TimeSpan.FromSeconds(5));

        // Act
        var result = await _enforcer.ExecuteWithTimeLimitsAsync(
            "task-1",
            policy,
            async _ =>
            {
                await Task.Delay(10, CancellationToken.None);
                return "result";
            },
            CancellationToken.None
        );

        // Assert
        Assert.Equal("result", result);
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_ExceedsSoftLimit_ThrowsSoftTimeLimitExceededException()
    {
        // Arrange
        var policy = new TimeLimitPolicy(SoftLimit: TimeSpan.FromMilliseconds(50));

        // Act & Assert
        await Assert.ThrowsAsync<SoftTimeLimitExceededException>(async () =>
        {
            await _enforcer.ExecuteWithTimeLimitsAsync(
                "task-1",
                policy,
                async _ =>
                {
                    await Task.Delay(500, CancellationToken.None);
                    return "result";
                },
                CancellationToken.None
            );
        });
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_ExceedsHardLimit_ThrowsTimeoutException()
    {
        // Arrange
        var policy = new TimeLimitPolicy(HardLimit: TimeSpan.FromMilliseconds(50));

        // Act & Assert
        await Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await _enforcer.ExecuteWithTimeLimitsAsync(
                "task-1",
                policy,
                async ct =>
                {
                    await Task.Delay(500, ct);
                    return "result";
                },
                CancellationToken.None
            );
        });
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_WithBothLimits_SoftTriggersFirst()
    {
        // Arrange
        var policy = new TimeLimitPolicy(
            SoftLimit: TimeSpan.FromMilliseconds(50),
            HardLimit: TimeSpan.FromMilliseconds(500)
        );

        // Act & Assert
        await Assert.ThrowsAsync<SoftTimeLimitExceededException>(async () =>
        {
            await _enforcer.ExecuteWithTimeLimitsAsync(
                "task-1",
                policy,
                async _ =>
                {
                    await Task.Delay(300, CancellationToken.None);
                    return "result";
                },
                CancellationToken.None
            );
        });
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_FastTask_CompletesBeforeSoftLimit()
    {
        // Arrange
        var policy = new TimeLimitPolicy(
            SoftLimit: TimeSpan.FromSeconds(5),
            HardLimit: TimeSpan.FromSeconds(10)
        );

        // Act
        var result = await _enforcer.ExecuteWithTimeLimitsAsync(
            "task-1",
            policy,
            async _ =>
            {
                await Task.Delay(10, CancellationToken.None);
                return "result";
            },
            CancellationToken.None
        );

        // Assert
        Assert.Equal("result", result);
    }

    [Fact]
    public async Task ExecuteWithTimeLimitsAsync_RespectsExternalCancellation()
    {
        // Arrange
        var policy = new TimeLimitPolicy(HardLimit: TimeSpan.FromSeconds(30));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await _enforcer.ExecuteWithTimeLimitsAsync(
                "task-1",
                policy,
                async ct =>
                {
                    await Task.Delay(500, ct);
                    return "result";
                },
                cts.Token
            );
        });
    }

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
