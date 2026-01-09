using DotCelery.Backend.InMemory.Batches;
using DotCelery.Core.Batches;

namespace DotCelery.Tests.Unit.Batches;

/// <summary>
/// Tests for <see cref="InMemoryBatchStore"/>.
/// </summary>
public sealed class InMemoryBatchStoreTests : IAsyncDisposable
{
    private readonly InMemoryBatchStore _store = new();

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
    }

    [Fact]
    public async Task CreateAsync_StoresBatch()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);

        // Act
        await _store.CreateAsync(batch);
        var retrieved = await _store.GetAsync("batch-1");

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal("batch-1", retrieved.Id);
        Assert.Equal(2, retrieved.TotalTasks);
    }

    [Fact]
    public async Task GetAsync_WithNonExistentId_ReturnsNull()
    {
        // Act
        var result = await _store.GetAsync("nonexistent");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task GetBatchIdForTaskAsync_ReturnsCorrectBatchId()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        var batchId1 = await _store.GetBatchIdForTaskAsync("task-1");
        var batchId2 = await _store.GetBatchIdForTaskAsync("task-2");
        var batchId3 = await _store.GetBatchIdForTaskAsync("task-3");

        // Assert
        Assert.Equal("batch-1", batchId1);
        Assert.Equal("batch-1", batchId2);
        Assert.Null(batchId3);
    }

    [Fact]
    public async Task MarkTaskCompletedAsync_UpdatesCompletedTaskIds()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        var updated = await _store.MarkTaskCompletedAsync("batch-1", "task-1");

        // Assert
        Assert.NotNull(updated);
        Assert.Single(updated.CompletedTaskIds);
        Assert.Contains("task-1", updated.CompletedTaskIds);
        Assert.Equal(BatchState.Processing, updated.State);
    }

    [Fact]
    public async Task MarkTaskCompletedAsync_AllTasksComplete_UpdatesState()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        await _store.MarkTaskCompletedAsync("batch-1", "task-1");
        var final = await _store.MarkTaskCompletedAsync("batch-1", "task-2");

        // Assert
        Assert.NotNull(final);
        Assert.Equal(BatchState.Completed, final.State);
        Assert.True(final.IsFinished);
        Assert.NotNull(final.CompletedAt);
    }

    [Fact]
    public async Task MarkTaskFailedAsync_UpdatesFailedTaskIds()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        var updated = await _store.MarkTaskFailedAsync("batch-1", "task-1");

        // Assert
        Assert.NotNull(updated);
        Assert.Single(updated.FailedTaskIds);
        Assert.Contains("task-1", updated.FailedTaskIds);
        Assert.Equal(BatchState.Processing, updated.State);
    }

    [Fact]
    public async Task MarkTaskFailedAsync_AllTasksFailed_UpdatesStateToFailed()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        await _store.MarkTaskFailedAsync("batch-1", "task-1");
        var final = await _store.MarkTaskFailedAsync("batch-1", "task-2");

        // Assert
        Assert.NotNull(final);
        Assert.Equal(BatchState.Failed, final.State);
        Assert.True(final.IsFinished);
    }

    [Fact]
    public async Task MarkTasks_MixedResults_UpdatesStateToPartiallyCompleted()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        await _store.MarkTaskCompletedAsync("batch-1", "task-1");
        var final = await _store.MarkTaskFailedAsync("batch-1", "task-2");

        // Assert
        Assert.NotNull(final);
        Assert.Equal(BatchState.PartiallyCompleted, final.State);
        Assert.True(final.IsFinished);
        Assert.Single(final.CompletedTaskIds);
        Assert.Single(final.FailedTaskIds);
    }

    [Fact]
    public async Task UpdateStateAsync_UpdatesBatchState()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        await _store.UpdateStateAsync("batch-1", BatchState.Cancelled);
        var updated = await _store.GetAsync("batch-1");

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(BatchState.Cancelled, updated.State);
        Assert.NotNull(updated.CompletedAt);
    }

    [Fact]
    public async Task DeleteAsync_RemovesBatch()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        var deleted = await _store.DeleteAsync("batch-1");
        var retrieved = await _store.GetAsync("batch-1");

        // Assert
        Assert.True(deleted);
        Assert.Null(retrieved);
    }

    [Fact]
    public async Task DeleteAsync_RemovesTaskMappings()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2"]);
        await _store.CreateAsync(batch);

        // Act
        await _store.DeleteAsync("batch-1");
        var batchId = await _store.GetBatchIdForTaskAsync("task-1");

        // Assert
        Assert.Null(batchId);
    }

    [Fact]
    public async Task DeleteAsync_WithNonExistentId_ReturnsFalse()
    {
        // Act
        var deleted = await _store.DeleteAsync("nonexistent");

        // Assert
        Assert.False(deleted);
    }

    [Fact]
    public async Task Batch_Progress_CalculatesCorrectly()
    {
        // Arrange
        var batch = CreateBatch("batch-1", ["task-1", "task-2", "task-3", "task-4"]);
        await _store.CreateAsync(batch);

        // Act
        await _store.MarkTaskCompletedAsync("batch-1", "task-1");
        var updated = await _store.MarkTaskFailedAsync("batch-1", "task-2");

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(50.0, updated.Progress);
        Assert.Equal(2, updated.PendingCount);
    }

    private static Batch CreateBatch(string id, string[] taskIds) =>
        new()
        {
            Id = id,
            Name = $"Test Batch {id}",
            State = BatchState.Pending,
            TaskIds = taskIds,
            CreatedAt = DateTimeOffset.UtcNow,
        };
}
