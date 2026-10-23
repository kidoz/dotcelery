using DotCelery.Core.Resilience;

namespace DotCelery.Tests.Unit.Worker;

public class RetryPolicyTests
{
    [Fact]
    public void ComputeDelay_Constant_ReturnsTheSameDelayForEveryRetry()
    {
        var policy = RetryPolicy.Constant(delay: TimeSpan.FromSeconds(5), jitter: false);

        Assert.Equal(TimeSpan.FromSeconds(5), policy.ComputeDelay(0));
        Assert.Equal(TimeSpan.FromSeconds(5), policy.ComputeDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(5), policy.ComputeDelay(7));
    }

    [Fact]
    public void ComputeDelay_Exponential_GrowsByTheMultiplier()
    {
        var policy = RetryPolicy.Exponential(
            initialDelay: TimeSpan.FromSeconds(1),
            multiplier: 2.0,
            jitter: false
        );

        Assert.Equal(TimeSpan.FromSeconds(1), policy.ComputeDelay(0));
        Assert.Equal(TimeSpan.FromSeconds(2), policy.ComputeDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(4), policy.ComputeDelay(2));
        Assert.Equal(TimeSpan.FromSeconds(8), policy.ComputeDelay(3));
    }

    [Fact]
    public void ComputeDelay_Exponential_IsCappedAtMaxDelay()
    {
        var policy = RetryPolicy.Exponential(
            initialDelay: TimeSpan.FromSeconds(1),
            maxDelay: TimeSpan.FromSeconds(5),
            multiplier: 2.0,
            jitter: false
        );

        Assert.Equal(TimeSpan.FromSeconds(4), policy.ComputeDelay(2));
        Assert.Equal(TimeSpan.FromSeconds(5), policy.ComputeDelay(3));
        Assert.Equal(TimeSpan.FromSeconds(5), policy.ComputeDelay(10));
    }

    [Fact]
    public void ComputeDelay_WithJitter_StaysWithinHalfToFullBaseDelay()
    {
        var policy = RetryPolicy.Exponential(
            initialDelay: TimeSpan.FromSeconds(4),
            multiplier: 2.0,
            jitter: true
        );

        for (var i = 0; i < 200; i++)
        {
            var delay = policy.ComputeDelay(1); // base delay: 8 seconds
            Assert.InRange(delay, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(8));
        }
    }

    [Fact]
    public void ShouldRetry_NoExceptionLists_RetriesEverything()
    {
        var policy = RetryPolicy.Default();

        Assert.True(policy.ShouldRetry(new InvalidOperationException()));
        Assert.True(policy.ShouldRetry(new TimeoutException()));
    }

    [Fact]
    public void ShouldRetry_RetryableExceptions_RetriesOnlyListedTypes()
    {
        var policy = RetryPolicy.Default() with
        {
            RetryableExceptions = [typeof(TimeoutException)],
        };

        Assert.True(policy.ShouldRetry(new TimeoutException()));
        Assert.False(policy.ShouldRetry(new InvalidOperationException()));
    }

    [Fact]
    public void ShouldRetry_NonRetryableExceptions_AlwaysWins()
    {
        var policy = RetryPolicy.Default() with
        {
            RetryableExceptions = [typeof(Exception)],
            NonRetryableExceptions = [typeof(InvalidOperationException)],
        };

        Assert.False(policy.ShouldRetry(new InvalidOperationException()));
        Assert.True(policy.ShouldRetry(new TimeoutException()));
    }

    [Fact]
    public void ShouldRetry_ListsMatchDerivedExceptions()
    {
        var policy = RetryPolicy.Default() with
        {
            NonRetryableExceptions = [typeof(InvalidOperationException)],
        };

        Assert.False(policy.ShouldRetry(new DerivedInvalidOperationException()));
    }

    [Fact]
    public void Validate_RejectsInvalidSettings()
    {
        Assert.Throws<ArgumentException>(() =>
            (RetryPolicy.Default() with { MaxRetries = -1 }).Validate()
        );
        Assert.Throws<ArgumentException>(() =>
            (RetryPolicy.Default() with { InitialDelay = TimeSpan.FromSeconds(-1) }).Validate()
        );
        Assert.Throws<ArgumentException>(() =>
            (RetryPolicy.Default() with { MaxDelay = TimeSpan.FromSeconds(-1) }).Validate()
        );
        Assert.Throws<ArgumentException>(() =>
            (
                RetryPolicy.Default() with
                {
                    InitialDelay = TimeSpan.FromSeconds(10),
                    MaxDelay = TimeSpan.FromSeconds(5),
                }
            ).Validate()
        );
        Assert.Throws<ArgumentException>(() =>
            (RetryPolicy.Default() with { BackoffMultiplier = 0.5 }).Validate()
        );
    }

    [Fact]
    public void Validate_AcceptsTheDefaultPolicy()
    {
        RetryPolicy.Default().Validate();
        RetryPolicy.None().Validate();
    }

    [Fact]
    public void None_DisablesRetries()
    {
        Assert.Equal(0, RetryPolicy.None().MaxRetries);
    }

    [Fact]
    public void Default_HasTheDocumentedDefaults()
    {
        var policy = RetryPolicy.Default();

        Assert.Equal(3, policy.MaxRetries);
        Assert.Equal(BackoffStrategy.Exponential, policy.Strategy);
        Assert.Equal(TimeSpan.FromSeconds(1), policy.InitialDelay);
        Assert.Equal(TimeSpan.FromSeconds(60), policy.MaxDelay);
        Assert.Equal(2.0, policy.BackoffMultiplier);
        Assert.True(policy.Jitter);
    }

    [Fact]
    public void Constant_SetsAConstantStrategy()
    {
        var policy = RetryPolicy.Constant(maxRetries: 5, delay: TimeSpan.FromSeconds(2));

        Assert.Equal(5, policy.MaxRetries);
        Assert.Equal(BackoffStrategy.Constant, policy.Strategy);
        Assert.Equal(TimeSpan.FromSeconds(2), policy.InitialDelay);
        Assert.Equal(TimeSpan.FromSeconds(2), policy.MaxDelay);
    }

    [Fact]
    public void Exponential_SetsAnExponentialStrategy()
    {
        var policy = RetryPolicy.Exponential(
            maxRetries: 4,
            initialDelay: TimeSpan.FromSeconds(2),
            maxDelay: TimeSpan.FromSeconds(30),
            multiplier: 3.0
        );

        Assert.Equal(4, policy.MaxRetries);
        Assert.Equal(BackoffStrategy.Exponential, policy.Strategy);
        Assert.Equal(TimeSpan.FromSeconds(2), policy.InitialDelay);
        Assert.Equal(TimeSpan.FromSeconds(30), policy.MaxDelay);
        Assert.Equal(3.0, policy.BackoffMultiplier);
    }

    private sealed class DerivedInvalidOperationException : InvalidOperationException;
}
