using DotCelery.Backend.InMemory.Partitioning;

namespace DotCelery.Tests.Unit.Partitioning;

/// <summary>
/// Tests for <see cref="InMemoryPartitionLockStore"/>.
/// </summary>
public sealed class InMemoryPartitionLockStoreTests : IAsyncDisposable
{
    private readonly InMemoryPartitionLockStore _store = new();

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
    }

    [Fact]
    public async Task TryAcquireAsync_WithNewPartition_ReturnsTrue()
    {
        // Act
        var result = await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TryAcquireAsync_WithSameTask_ReturnsTrue()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Act - same task tries to acquire again
        var result = await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TryAcquireAsync_WithDifferentTask_ReturnsFalse()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Act - different task tries to acquire
        var result = await _store.TryAcquireAsync("partition-1", "task-2", TimeSpan.FromMinutes(5));

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task TryAcquireAsync_AfterRelease_AllowsNewTask()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));
        await _store.ReleaseAsync("partition-1", "task-1");

        // Act
        var result = await _store.TryAcquireAsync("partition-1", "task-2", TimeSpan.FromMinutes(5));

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task ReleaseAsync_WithCorrectTask_ReturnsTrue()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Act
        var result = await _store.ReleaseAsync("partition-1", "task-1");

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task ReleaseAsync_WithWrongTask_ReturnsFalse()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Act
        var result = await _store.ReleaseAsync("partition-1", "task-2");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ReleaseAsync_WithNonExistentPartition_ReturnsFalse()
    {
        // Act
        var result = await _store.ReleaseAsync("nonexistent", "task-1");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task IsLockedAsync_WithLockedPartition_ReturnsTrue()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Act
        var result = await _store.IsLockedAsync("partition-1");

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task IsLockedAsync_WithUnlockedPartition_ReturnsFalse()
    {
        // Act
        var result = await _store.IsLockedAsync("partition-1");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task GetLockHolderAsync_WithLockedPartition_ReturnsTaskId()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Act
        var result = await _store.GetLockHolderAsync("partition-1");

        // Assert
        Assert.Equal("task-1", result);
    }

    [Fact]
    public async Task GetLockHolderAsync_WithUnlockedPartition_ReturnsNull()
    {
        // Act
        var result = await _store.GetLockHolderAsync("partition-1");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ExtendAsync_WithCorrectTask_ReturnsTrue()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(1));

        // Act
        var result = await _store.ExtendAsync("partition-1", "task-1", TimeSpan.FromMinutes(10));

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task ExtendAsync_WithWrongTask_ReturnsFalse()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(1));

        // Act
        var result = await _store.ExtendAsync("partition-1", "task-2", TimeSpan.FromMinutes(10));

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task TryAcquireAsync_AfterTimeout_AllowsNewTask()
    {
        // Arrange - acquire with very short timeout
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMilliseconds(50));

        // Wait for timeout
        await Task.Delay(100);

        // Act
        var result = await _store.TryAcquireAsync("partition-1", "task-2", TimeSpan.FromMinutes(5));

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task MultiplePartitions_AreIndependent()
    {
        // Arrange
        await _store.TryAcquireAsync("partition-1", "task-1", TimeSpan.FromMinutes(5));

        // Act - acquire different partition
        var result = await _store.TryAcquireAsync("partition-2", "task-2", TimeSpan.FromMinutes(5));

        // Assert
        Assert.True(result);
    }
}
