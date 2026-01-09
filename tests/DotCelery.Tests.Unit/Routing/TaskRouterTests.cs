using DotCelery.Core.Routing;

namespace DotCelery.Tests.Unit.Routing;

/// <summary>
/// Tests for <see cref="TaskRouter"/>.
/// </summary>
public sealed class TaskRouterTests
{
    private readonly TaskRouter _router = new();

    [Fact]
    public void GetQueue_WithNoRoutes_ReturnsDefaultQueue()
    {
        // Act
        var queue = _router.GetQueue("any.task", "default");

        // Assert
        Assert.Equal("default", queue);
    }

    [Fact]
    public void AddRoute_WithExactMatch_ReturnsConfiguredQueue()
    {
        // Arrange
        _router.AddRoute("email.send", "email-queue");

        // Act
        var queue = _router.GetQueue("email.send", "default");

        // Assert
        Assert.Equal("email-queue", queue);
    }

    [Fact]
    public void AddRoute_WithSingleWildcard_MatchesSingleSegment()
    {
        // Arrange
        _router.AddRoute("reports.*", "reports-queue");

        // Act
        var queue1 = _router.GetQueue("reports.daily", "default");
        var queue2 = _router.GetQueue("reports.weekly", "default");

        // Assert
        Assert.Equal("reports-queue", queue1);
        Assert.Equal("reports-queue", queue2);
    }

    [Fact]
    public void AddRoute_WithSingleWildcard_DoesNotMatchMultipleSegments()
    {
        // Arrange
        _router.AddRoute("reports.*", "reports-queue");

        // Act
        var queue = _router.GetQueue("reports.email.send", "default");

        // Assert
        Assert.Equal("default", queue);
    }

    [Fact]
    public void AddRoute_WithDoubleWildcard_MatchesMultipleSegments()
    {
        // Arrange
        _router.AddRoute("reports.**", "reports-queue");

        // Act
        var queue1 = _router.GetQueue("reports.daily", "default");
        var queue2 = _router.GetQueue("reports.email.send", "default");
        var queue3 = _router.GetQueue("reports.pdf.generate.async", "default");

        // Assert
        Assert.Equal("reports-queue", queue1);
        Assert.Equal("reports-queue", queue2);
        Assert.Equal("reports-queue", queue3);
    }

    [Fact]
    public void AddRoute_WithMiddleWildcard_MatchesPattern()
    {
        // Arrange
        _router.AddRoute("api.*.handler", "api-queue");

        // Act
        var queue1 = _router.GetQueue("api.user.handler", "default");
        var queue2 = _router.GetQueue("api.order.handler", "default");
        var queue3 = _router.GetQueue("api.user.service", "default");

        // Assert
        Assert.Equal("api-queue", queue1);
        Assert.Equal("api-queue", queue2);
        Assert.Equal("default", queue3);
    }

    [Fact]
    public void AddRoute_ExactMatchHasPriority()
    {
        // Arrange
        _router.AddRoute("reports.*", "reports-queue");
        _router.AddRoute("reports.critical", "critical-queue");

        // Act
        var queue1 = _router.GetQueue("reports.daily", "default");
        var queue2 = _router.GetQueue("reports.critical", "default");

        // Assert
        Assert.Equal("reports-queue", queue1);
        Assert.Equal("critical-queue", queue2);
    }

    [Fact]
    public void AddRoute_MoreSpecificPatternHasPriority()
    {
        // Arrange
        _router.AddRoute("**", "catch-all");
        _router.AddRoute("reports.*", "reports-queue");

        // Act
        var queue1 = _router.GetQueue("reports.daily", "default");
        var queue2 = _router.GetQueue("other.task", "default");

        // Assert
        Assert.Equal("reports-queue", queue1);
        Assert.Equal("catch-all", queue2);
    }

    [Fact]
    public void GetQueue_WithMultipleMatchingPatterns_ReturnsFirstMatch()
    {
        // Arrange - more specific first
        _router.AddRoute("email.**", "email-queue");
        _router.AddRoute("**", "catch-all");

        // Act
        var queue = _router.GetQueue("email.send.notification", "default");

        // Assert
        Assert.Equal("email-queue", queue);
    }

    [Fact]
    public void AddRoute_WithEmptyPattern_ThrowsArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => _router.AddRoute("", "queue"));
    }

    [Fact]
    public void AddRoute_WithEmptyQueue_ThrowsArgumentException()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => _router.AddRoute("pattern", ""));
    }

    [Fact]
    public void GetQueue_WithNullTaskName_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => _router.GetQueue(null!, "default"));
    }
}
