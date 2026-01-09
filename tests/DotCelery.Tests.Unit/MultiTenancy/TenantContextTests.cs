using DotCelery.Core.MultiTenancy;

namespace DotCelery.Tests.Unit.MultiTenancy;

/// <summary>
/// Tests for <see cref="TenantContext"/>.
/// </summary>
public sealed class TenantContextTests
{
    [Fact]
    public void TenantId_WhenNotSet_ReturnsNull()
    {
        // Act
        var tenantId = TenantContext.Current.TenantId;

        // Assert
        Assert.Null(tenantId);
    }

    [Fact]
    public void HasTenant_WhenNotSet_ReturnsFalse()
    {
        // Act
        var hasTenant = TenantContext.Current.HasTenant;

        // Assert
        Assert.False(hasTenant);
    }

    [Fact]
    public void SetTenant_SetsTenantId()
    {
        // Act
        using (TenantContext.SetTenant("tenant-1"))
        {
            // Assert
            Assert.Equal("tenant-1", TenantContext.Current.TenantId);
            Assert.True(TenantContext.Current.HasTenant);
        }
    }

    [Fact]
    public void SetTenant_RestoresPreviousTenant_OnDispose()
    {
        // Act
        using (TenantContext.SetTenant("tenant-1"))
        {
            Assert.Equal("tenant-1", TenantContext.Current.TenantId);

            using (TenantContext.SetTenant("tenant-2"))
            {
                Assert.Equal("tenant-2", TenantContext.Current.TenantId);
            }

            // After inner dispose, should restore to tenant-1
            Assert.Equal("tenant-1", TenantContext.Current.TenantId);
        }

        // After outer dispose, should be null again
        Assert.Null(TenantContext.Current.TenantId);
    }

    [Fact]
    public void SetTenant_WithNull_SetsNullTenantId()
    {
        // Arrange
        using (TenantContext.SetTenant("tenant-1"))
        {
            // Act
            using (TenantContext.SetTenant(null))
            {
                // Assert
                Assert.Null(TenantContext.Current.TenantId);
                Assert.False(TenantContext.Current.HasTenant);
            }

            // Restore to tenant-1
            Assert.Equal("tenant-1", TenantContext.Current.TenantId);
        }
    }

    [Fact]
    public void HasTenant_WithEmptyString_ReturnsFalse()
    {
        // Act
        using (TenantContext.SetTenant(""))
        {
            // Assert
            Assert.False(TenantContext.Current.HasTenant);
        }
    }

    [Fact]
    public async Task TenantContext_FlowsAcrossAsync()
    {
        // Arrange
        string? tenantInsideTask = null;

        // Act
        using (TenantContext.SetTenant("tenant-async"))
        {
            await Task.Run(() =>
            {
                tenantInsideTask = TenantContext.Current.TenantId;
            });
        }

        // Assert
        Assert.Equal("tenant-async", tenantInsideTask);
    }

    [Fact]
    public void Current_ReturnsSameInstance()
    {
        // Act
        var instance1 = TenantContext.Current;
        var instance2 = TenantContext.Current;

        // Assert
        Assert.Same(instance1, instance2);
    }

    [Fact]
    public void ITenantContext_ImplementedCorrectly()
    {
        // Arrange
        var context = TenantContext.Current;

        // Act
        using (TenantContext.SetTenant("tenant-interface"))
        {
            // Assert
            Assert.Equal("tenant-interface", context.TenantId);
            Assert.True(context.HasTenant);
        }

        Assert.Null(context.TenantId);
        Assert.False(context.HasTenant);
    }
}
