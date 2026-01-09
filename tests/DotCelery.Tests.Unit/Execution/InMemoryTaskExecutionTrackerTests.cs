using DotCelery.Backend.InMemory.Execution;

namespace DotCelery.Tests.Unit.Execution;

/// <summary>
/// Tests for <see cref="InMemoryTaskExecutionTracker"/>.
/// </summary>
public sealed class InMemoryTaskExecutionTrackerTests : IAsyncDisposable
{
    private readonly InMemoryTaskExecutionTracker _tracker = new();

    public async ValueTask DisposeAsync()
    {
        await _tracker.DisposeAsync();
    }

    [Fact]
    public async Task TryStartAsync_WithNewTask_ReturnsTrue()
    {
        // Act
        var result = await _tracker.TryStartAsync("task.name", "task-1", null, null);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TryStartAsync_WithSameTaskName_ReturnsFalse()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", null, null);

        // Act - same task name with different id
        var result = await _tracker.TryStartAsync("task.name", "task-2", null, null);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task TryStartAsync_WithDifferentKey_ReturnsTrue()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", "key-1", null);

        // Act - same task name but different key
        var result = await _tracker.TryStartAsync("task.name", "task-2", "key-2", null);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TryStartAsync_WithSameKey_ReturnsFalse()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", "key-1", null);

        // Act - same task name and same key
        var result = await _tracker.TryStartAsync("task.name", "task-2", "key-1", null);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task StopAsync_ReleasesLock()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", null, null);

        // Act
        await _tracker.StopAsync("task.name", "task-1", null);
        var result = await _tracker.TryStartAsync("task.name", "task-2", null, null);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task IsExecutingAsync_WithRunningTask_ReturnsTrue()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", null, null);

        // Act
        var result = await _tracker.IsExecutingAsync("task.name", null);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task IsExecutingAsync_WithNoRunningTask_ReturnsFalse()
    {
        // Act
        var result = await _tracker.IsExecutingAsync("task.name", null);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task IsExecutingAsync_WithKey_ChecksCorrectKey()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", "key-1", null);

        // Act
        var resultKey1 = await _tracker.IsExecutingAsync("task.name", "key-1");
        var resultKey2 = await _tracker.IsExecutingAsync("task.name", "key-2");

        // Assert
        Assert.True(resultKey1);
        Assert.False(resultKey2);
    }

    [Fact]
    public async Task GetExecutingTaskIdAsync_ReturnsCorrectTaskId()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", null, null);

        // Act
        var result = await _tracker.GetExecutingTaskIdAsync("task.name", null);

        // Assert
        Assert.Equal("task-1", result);
    }

    [Fact]
    public async Task GetExecutingTaskIdAsync_WithNoRunningTask_ReturnsNull()
    {
        // Act
        var result = await _tracker.GetExecutingTaskIdAsync("task.name", null);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ExtendAsync_WithCorrectTask_ReturnsTrue()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", null, TimeSpan.FromMinutes(1));

        // Act
        var result = await _tracker.ExtendAsync(
            "task.name",
            "task-1",
            null,
            TimeSpan.FromMinutes(10)
        );

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task ExtendAsync_WithWrongTask_ReturnsFalse()
    {
        // Arrange
        await _tracker.TryStartAsync("task.name", "task-1", null, TimeSpan.FromMinutes(1));

        // Act
        var result = await _tracker.ExtendAsync(
            "task.name",
            "task-2",
            null,
            TimeSpan.FromMinutes(10)
        );

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task GetAllExecutingAsync_ReturnsAllRunningTasks()
    {
        // Arrange
        await _tracker.TryStartAsync("task.one", "task-1", null, null);
        await _tracker.TryStartAsync("task.two", "task-2", null, null);

        // Act
        var result = await _tracker.GetAllExecutingAsync();

        // Assert
        Assert.Equal(2, result.Count);
        Assert.Contains("task.one", result.Keys);
        Assert.Contains("task.two", result.Keys);
    }

    [Fact]
    public async Task TryStartAsync_AfterTimeout_AllowsNewTask()
    {
        // Arrange - start with short timeout
        await _tracker.TryStartAsync("task.name", "task-1", null, TimeSpan.FromMilliseconds(50));

        // Wait for timeout
        await Task.Delay(100);

        // Act
        var result = await _tracker.TryStartAsync("task.name", "task-2", null, null);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task DifferentTaskNames_AreIndependent()
    {
        // Arrange
        await _tracker.TryStartAsync("task.one", "task-1", null, null);

        // Act - different task name
        var result = await _tracker.TryStartAsync("task.two", "task-2", null, null);

        // Assert
        Assert.True(result);
    }
}
