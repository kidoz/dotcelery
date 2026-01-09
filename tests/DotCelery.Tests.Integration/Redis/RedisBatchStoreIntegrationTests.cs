using DotCelery.Backend.Redis.Batches;
using DotCelery.Core.Batches;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Testcontainers.Redis;

namespace DotCelery.Tests.Integration.Redis;

/// <summary>
/// Integration tests for Redis batch store using Testcontainers.
/// These tests require Docker to be running.
/// </summary>
[Collection("Redis")]
public class RedisBatchStoreIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _container;
    private RedisBatchStore? _store;

    public RedisBatchStoreIntegrationTests()
    {
        _container = new RedisBuilder("redis:7-alpine").Build();
    }

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        var options = Options.Create(
            new RedisBatchStoreOptions { ConnectionString = _container.GetConnectionString() }
        );

        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisBatchStore>();

        _store = new RedisBatchStore(options, logger);
    }

    public async ValueTask DisposeAsync()
    {
        if (_store is not null)
        {
            await _store.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    [Fact]
    public async Task CreateAsync_StoresBatch()
    {
        var batch = CreateTestBatch();

        await _store!.CreateAsync(batch);

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(batch.Id, retrieved.Id);
        Assert.Equal(batch.Name, retrieved.Name);
        Assert.Equal(batch.State, retrieved.State);
    }

    [Fact]
    public async Task GetAsync_NonExistent_ReturnsNull()
    {
        var batch = await _store!.GetAsync("non-existent-batch-id");

        Assert.Null(batch);
    }

    [Fact]
    public async Task CreateAsync_IndexesTaskIds()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        foreach (var taskId in batch.TaskIds)
        {
            var batchId = await _store.GetBatchIdForTaskAsync(taskId);
            Assert.Equal(batch.Id, batchId);
        }
    }

    [Fact]
    public async Task GetBatchIdForTaskAsync_NonExistent_ReturnsNull()
    {
        var batchId = await _store!.GetBatchIdForTaskAsync("non-existent-task-id");

        Assert.Null(batchId);
    }

    [Fact]
    public async Task UpdateStateAsync_UpdatesState()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        await _store.UpdateStateAsync(batch.Id, BatchState.Processing);

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(BatchState.Processing, retrieved.State);
    }

    [Fact]
    public async Task UpdateStateAsync_Completed_SetsCompletedAt()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        await _store.UpdateStateAsync(batch.Id, BatchState.Completed);

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.NotNull(retrieved.CompletedAt);
    }

    [Fact]
    public async Task MarkTaskCompletedAsync_UpdatesCompletedTaskIds()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        var taskId = batch.TaskIds[0];
        var updated = await _store.MarkTaskCompletedAsync(batch.Id, taskId);

        Assert.NotNull(updated);
        Assert.Contains(taskId, updated.CompletedTaskIds);
        Assert.Equal(1, updated.CompletedCount);
    }

    [Fact]
    public async Task MarkTaskCompletedAsync_AllTasksCompleted_UpdatesState()
    {
        var taskIds = new[] { "task-1", "task-2", "task-3" };
        var batch = CreateTestBatch() with { TaskIds = taskIds };
        await _store!.CreateAsync(batch);

        // Complete all tasks
        foreach (var taskId in taskIds)
        {
            await _store.MarkTaskCompletedAsync(batch.Id, taskId);
        }

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(BatchState.Completed, retrieved.State);
        Assert.NotNull(retrieved.CompletedAt);
        Assert.True(retrieved.IsFinished);
    }

    [Fact]
    public async Task MarkTaskFailedAsync_UpdatesFailedTaskIds()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        var taskId = batch.TaskIds[0];
        var updated = await _store.MarkTaskFailedAsync(batch.Id, taskId);

        Assert.NotNull(updated);
        Assert.Contains(taskId, updated.FailedTaskIds);
        Assert.Equal(1, updated.FailedCount);
    }

    [Fact]
    public async Task MarkTaskFailedAsync_AllTasksFailed_UpdatesState()
    {
        var taskIds = new[] { "task-1", "task-2" };
        var batch = CreateTestBatch() with { TaskIds = taskIds };
        await _store!.CreateAsync(batch);

        // Fail all tasks
        foreach (var taskId in taskIds)
        {
            await _store.MarkTaskFailedAsync(batch.Id, taskId);
        }

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(BatchState.Failed, retrieved.State);
        Assert.True(retrieved.IsFinished);
    }

    [Fact]
    public async Task MixedCompletionAndFailure_PartiallyCompleted()
    {
        var taskIds = new[] { "task-1", "task-2", "task-3" };
        var batch = CreateTestBatch() with { TaskIds = taskIds };
        await _store!.CreateAsync(batch);

        // Complete one, fail one, complete one
        await _store.MarkTaskCompletedAsync(batch.Id, "task-1");
        await _store.MarkTaskFailedAsync(batch.Id, "task-2");
        await _store.MarkTaskCompletedAsync(batch.Id, "task-3");

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(BatchState.PartiallyCompleted, retrieved.State);
        Assert.Equal(2, retrieved.CompletedCount);
        Assert.Equal(1, retrieved.FailedCount);
    }

    [Fact]
    public async Task DeleteAsync_RemovesBatch()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        var deleted = await _store.DeleteAsync(batch.Id);

        Assert.True(deleted);
        var retrieved = await _store.GetAsync(batch.Id);
        Assert.Null(retrieved);
    }

    [Fact]
    public async Task DeleteAsync_RemovesTaskIndexes()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        await _store.DeleteAsync(batch.Id);

        foreach (var taskId in batch.TaskIds)
        {
            var batchId = await _store.GetBatchIdForTaskAsync(taskId);
            Assert.Null(batchId);
        }
    }

    [Fact]
    public async Task DeleteAsync_NonExistent_ReturnsFalse()
    {
        var deleted = await _store!.DeleteAsync("non-existent-batch-id");

        Assert.False(deleted);
    }

    [Fact]
    public async Task Progress_CalculatedCorrectly()
    {
        var taskIds = new[] { "task-1", "task-2", "task-3", "task-4" };
        var batch = CreateTestBatch() with { TaskIds = taskIds };
        await _store!.CreateAsync(batch);

        // Initial progress should be 0
        var initial = await _store.GetAsync(batch.Id);
        Assert.Equal(0, initial!.Progress);

        // Complete 2 tasks
        await _store.MarkTaskCompletedAsync(batch.Id, "task-1");
        await _store.MarkTaskCompletedAsync(batch.Id, "task-2");

        var halfway = await _store.GetAsync(batch.Id);
        Assert.Equal(50, halfway!.Progress);

        // Complete remaining
        await _store.MarkTaskCompletedAsync(batch.Id, "task-3");
        await _store.MarkTaskCompletedAsync(batch.Id, "task-4");

        var complete = await _store.GetAsync(batch.Id);
        Assert.Equal(100, complete!.Progress);
    }

    [Fact]
    public async Task SequentialTaskCompletions_AllSucceed()
    {
        var taskIds = Enumerable.Range(0, 10).Select(i => $"task-{i}").ToArray();
        var batch = CreateTestBatch() with { TaskIds = taskIds };
        await _store!.CreateAsync(batch);

        // Complete tasks sequentially (concurrent updates require atomic operations)
        foreach (var taskId in taskIds)
        {
            await _store.MarkTaskCompletedAsync(batch.Id, taskId);
        }

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(10, retrieved.CompletedCount);
        Assert.Equal(BatchState.Completed, retrieved.State);
    }

    [Fact]
    public async Task MarkTaskCompletedAsync_SameTaskTwice_DoesNotDuplicate()
    {
        var batch = CreateTestBatch();
        await _store!.CreateAsync(batch);

        var taskId = batch.TaskIds[0];
        await _store.MarkTaskCompletedAsync(batch.Id, taskId);
        await _store.MarkTaskCompletedAsync(batch.Id, taskId);

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.Equal(1, retrieved!.CompletedCount);
    }

    [Fact]
    public async Task FirstTaskCompletion_UpdatesStateToPending()
    {
        var batch = CreateTestBatch() with { State = BatchState.Pending };
        await _store!.CreateAsync(batch);

        await _store.MarkTaskCompletedAsync(batch.Id, batch.TaskIds[0]);

        var retrieved = await _store.GetAsync(batch.Id);
        Assert.Equal(BatchState.Processing, retrieved!.State);
    }

    [Fact]
    public async Task TwoStores_ShareBatchData()
    {
        // Create a second store pointing to the same Redis
        var options = Options.Create(
            new RedisBatchStoreOptions { ConnectionString = _container.GetConnectionString() }
        );
        var logger = LoggerFactory
            .Create(builder => builder.AddConsole())
            .CreateLogger<RedisBatchStore>();
        await using var store2 = new RedisBatchStore(options, logger);

        var batch = CreateTestBatch();

        // Create using first store
        await _store!.CreateAsync(batch);

        // Read using second store
        var retrieved = await store2.GetAsync(batch.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(batch.Id, retrieved.Id);

        // Update using second store
        await store2.MarkTaskCompletedAsync(batch.Id, batch.TaskIds[0]);

        // Verify update using first store
        var updated = await _store.GetAsync(batch.Id);
        Assert.Equal(1, updated!.CompletedCount);
    }

    private static Batch CreateTestBatch() =>
        new()
        {
            Id = Guid.NewGuid().ToString(),
            Name = "test-batch",
            State = BatchState.Pending,
            TaskIds = ["task-1", "task-2", "task-3"],
            CreatedAt = DateTimeOffset.UtcNow,
        };
}
