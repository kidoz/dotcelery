namespace DotCelery.Tests.Unit.Client;

using DotCelery.Client;

public class SendOptionsTests
{
    [Fact]
    public void Validate_DefaultOptions_DoesNotThrow()
    {
        var options = new SendOptions();

        var exception = Record.Exception(() => options.Validate());

        Assert.Null(exception);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(9)]
    public void Validate_ValidPriority_DoesNotThrow(int priority)
    {
        var options = new SendOptions { Priority = priority };

        var exception = Record.Exception(() => options.Validate());

        Assert.Null(exception);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    [InlineData(100)]
    public void Validate_InvalidPriority_ThrowsArgumentOutOfRangeException(int priority)
    {
        var options = new SendOptions { Priority = priority };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.Equal("Priority", exception.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(100)]
    public void Validate_ValidMaxRetries_DoesNotThrow(int maxRetries)
    {
        var options = new SendOptions { MaxRetries = maxRetries };

        var exception = Record.Exception(() => options.Validate());

        Assert.Null(exception);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(-100)]
    public void Validate_NegativeMaxRetries_ThrowsArgumentOutOfRangeException(int maxRetries)
    {
        var options = new SendOptions { MaxRetries = maxRetries };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.Equal("MaxRetries", exception.ParamName);
    }

    [Fact]
    public void Validate_ValidCountdown_DoesNotThrow()
    {
        var options = new SendOptions { Countdown = TimeSpan.FromMinutes(5) };

        var exception = Record.Exception(() => options.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_ZeroCountdown_DoesNotThrow()
    {
        var options = new SendOptions { Countdown = TimeSpan.Zero };

        var exception = Record.Exception(() => options.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_NegativeCountdown_ThrowsArgumentOutOfRangeException()
    {
        var options = new SendOptions { Countdown = TimeSpan.FromSeconds(-1) };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.Equal("Countdown", exception.ParamName);
    }

    [Fact]
    public void Validate_ExpiresBeforeEta_ThrowsArgumentException()
    {
        var now = DateTimeOffset.UtcNow;
        var options = new SendOptions { Eta = now.AddMinutes(10), Expires = now.AddMinutes(5) };

        var exception = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.Equal("Expires", exception.ParamName);
    }

    [Fact]
    public void Validate_ExpiresAfterEta_DoesNotThrow()
    {
        var now = DateTimeOffset.UtcNow;
        var options = new SendOptions { Eta = now.AddMinutes(5), Expires = now.AddMinutes(10) };

        var exception = Record.Exception(() => options.Validate());

        Assert.Null(exception);
    }

    [Fact]
    public void Validate_ExpiresWithCountdown_ValidatesAgainstCalculatedEta()
    {
        var options = new SendOptions
        {
            Countdown = TimeSpan.FromMinutes(10),
            Expires = DateTimeOffset.UtcNow.AddMinutes(5), // Expires before countdown completes
        };

        var exception = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.Equal("Expires", exception.ParamName);
    }

    [Fact]
    public void Validate_AllOptionsValid_DoesNotThrow()
    {
        var now = DateTimeOffset.UtcNow;
        var options = new SendOptions
        {
            Priority = 5,
            MaxRetries = 3,
            Countdown = TimeSpan.FromMinutes(1),
            Expires = now.AddMinutes(10),
            Queue = "custom-queue",
            TaskId = "custom-id",
            CorrelationId = "correlation-123",
        };

        var exception = Record.Exception(() => options.Validate());

        Assert.Null(exception);
    }
}
