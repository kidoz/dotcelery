using DotCelery.Core.MultiTenancy;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.MultiTenancy;

/// <summary>
/// Tests for <see cref="TenantRouter"/>.
/// </summary>
public sealed class TenantRouterTests
{
    [Fact]
    public void GetQueue_WithSuffixStrategy_AppendsTenantId()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Suffix };
        var router = new TenantRouter(Options.Create(options));

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("celery-tenant-1", queue);
    }

    [Fact]
    public void GetQueue_WithPrefixStrategy_PrependsTenantId()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Prefix };
        var router = new TenantRouter(Options.Create(options));

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("tenant-1-celery", queue);
    }

    [Fact]
    public void GetQueue_WithPathStrategy_UsesDotSeparator()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Path };
        var router = new TenantRouter(Options.Create(options));

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("tenant-1.celery", queue);
    }

    [Fact]
    public void GetQueue_WithCustomSeparator_UsesSeparator()
    {
        // Arrange
        var options = new MultiTenancyOptions
        {
            QueueStrategy = TenantQueueStrategy.Suffix,
            Separator = "_",
        };
        var router = new TenantRouter(Options.Create(options));

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("celery_tenant-1", queue);
    }

    [Fact]
    public void GetQueue_WithCustomMapping_UsesCustomQueue()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Suffix };
        var router = new TenantRouter(Options.Create(options));
        router.AddTenantQueue("tenant-1", "custom-queue");

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("custom-queue", queue);
    }

    [Fact]
    public void GetQueue_WithCustomStrategy_RequiresMapping()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Custom };
        var router = new TenantRouter(Options.Create(options));

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => router.GetQueue("tenant-1", "celery"));
    }

    [Fact]
    public void GetQueue_WithCustomStrategy_AndMapping_Succeeds()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Custom };
        var router = new TenantRouter(Options.Create(options));
        router.AddTenantQueue("tenant-1", "custom-queue");

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("custom-queue", queue);
    }

    [Fact]
    public void GetQueue_WithValidation_AcceptsValidTenant()
    {
        // Arrange
        var options = new MultiTenancyOptions
        {
            ValidateTenants = true,
            ValidTenants = ["tenant-1", "tenant-2"],
        };
        var router = new TenantRouter(Options.Create(options));

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("celery-tenant-1", queue);
    }

    [Fact]
    public void GetQueue_WithValidation_RejectsInvalidTenant()
    {
        // Arrange
        var options = new MultiTenancyOptions
        {
            ValidateTenants = true,
            ValidTenants = ["tenant-1", "tenant-2"],
        };
        var router = new TenantRouter(Options.Create(options));

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => router.GetQueue("unknown-tenant", "celery"));
    }

    [Fact]
    public void GetQueue_WithPrefix_AppliesPrefix()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Suffix };
        var router = new TenantRouter(Options.Create(options));
        router.SetQueuePrefix("prod_");

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("prod_celery-tenant-1", queue);
    }

    [Fact]
    public void GetQueue_WithSuffix_AppliesSuffix()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Suffix };
        var router = new TenantRouter(Options.Create(options));
        router.SetQueueSuffix("_v2");

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("celery_v2-tenant-1", queue);
    }

    [Fact]
    public void GetQueue_WithPrefixAndSuffix_AppliesBoth()
    {
        // Arrange
        var options = new MultiTenancyOptions { QueueStrategy = TenantQueueStrategy.Suffix };
        var router = new TenantRouter(Options.Create(options));
        router.SetQueuePrefix("prod_");
        router.SetQueueSuffix("_v2");

        // Act
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("prod_celery_v2-tenant-1", queue);
    }

    [Fact]
    public void GetTenantQueues_ReturnsAllMappings()
    {
        // Arrange
        var options = new MultiTenancyOptions();
        var router = new TenantRouter(Options.Create(options));
        router.AddTenantQueue("tenant-1", "queue-1");
        router.AddTenantQueue("tenant-2", "queue-2");

        // Act
        var queues = router.GetTenantQueues();

        // Assert
        Assert.Equal(2, queues.Count);
        Assert.Equal("queue-1", queues["tenant-1"]);
        Assert.Equal("queue-2", queues["tenant-2"]);
    }

    [Fact]
    public void AddTenantQueue_OverwritesExisting()
    {
        // Arrange
        var options = new MultiTenancyOptions();
        var router = new TenantRouter(Options.Create(options));
        router.AddTenantQueue("tenant-1", "queue-1");

        // Act
        router.AddTenantQueue("tenant-1", "queue-new");
        var queue = router.GetQueue("tenant-1", "celery");

        // Assert
        Assert.Equal("queue-new", queue);
    }

    [Fact]
    public void GetQueue_WithNullTenantId_ThrowsArgumentException()
    {
        // Arrange
        var options = new MultiTenancyOptions();
        var router = new TenantRouter(Options.Create(options));

        // Act & Assert
        Assert.ThrowsAny<ArgumentException>(() => router.GetQueue(null!, "celery"));
    }

    [Fact]
    public void GetQueue_WithEmptyTenantId_ThrowsArgumentException()
    {
        // Arrange
        var options = new MultiTenancyOptions();
        var router = new TenantRouter(Options.Create(options));

        // Act & Assert
        Assert.Throws<ArgumentException>(() => router.GetQueue("", "celery"));
    }
}
