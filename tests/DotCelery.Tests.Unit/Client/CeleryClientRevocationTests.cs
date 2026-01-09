namespace DotCelery.Tests.Unit.Client;

using DotCelery.Backend.InMemory;
using DotCelery.Backend.InMemory.Revocation;
using DotCelery.Broker.InMemory;
using DotCelery.Client;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

public class CeleryClientRevocationTests : IAsyncDisposable
{
    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryResultBackend _backend = new();
    private readonly InMemoryRevocationStore _revocationStore = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly CeleryClient _client;

    public CeleryClientRevocationTests()
    {
        var options = Options.Create(new CeleryClientOptions());
        _client = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            options,
            NullLogger<CeleryClient>.Instance,
            _revocationStore
        );
    }

    [Fact]
    public async Task RevokeAsync_PendingTask_MarksAsRevoked()
    {
        var taskId = "pending-task";
        await _backend.UpdateStateAsync(taskId, TaskState.Pending);

        await _client.RevokeAsync(taskId);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Revoked, state);
        Assert.True(await _revocationStore.IsRevokedAsync(taskId));
    }

    [Fact]
    public async Task RevokeAsync_StartedTask_MarksAsRevoked()
    {
        var taskId = "started-task";
        await _backend.UpdateStateAsync(taskId, TaskState.Started);

        await _client.RevokeAsync(taskId);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Revoked, state);
    }

    [Fact]
    public async Task RevokeAsync_SuccessTask_DoesNotChangeState()
    {
        var taskId = "success-task";
        await _backend.StoreResultAsync(
            new TaskResult
            {
                TaskId = taskId,
                State = TaskState.Success,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromSeconds(1),
            }
        );

        await _client.RevokeAsync(taskId);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Success, state);
        // But it should still be marked in the revocation store
        Assert.True(await _revocationStore.IsRevokedAsync(taskId));
    }

    [Fact]
    public async Task RevokeAsync_FailedTask_DoesNotChangeState()
    {
        var taskId = "failed-task";
        await _backend.StoreResultAsync(
            new TaskResult
            {
                TaskId = taskId,
                State = TaskState.Failure,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.FromSeconds(1),
            }
        );

        await _client.RevokeAsync(taskId);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Failure, state);
    }

    [Fact]
    public async Task RevokeAsync_RevokedTask_DoesNotChangeState()
    {
        var taskId = "revoked-task";
        await _backend.UpdateStateAsync(taskId, TaskState.Revoked);

        await _client.RevokeAsync(taskId);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Revoked, state);
    }

    [Fact]
    public async Task RevokeAsync_RejectedTask_DoesNotChangeState()
    {
        var taskId = "rejected-task";
        await _backend.UpdateStateAsync(taskId, TaskState.Rejected);

        await _client.RevokeAsync(taskId);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Rejected, state);
    }

    [Fact]
    public async Task RevokeAsync_NonExistentTask_CreatesRevokedState()
    {
        var taskId = "non-existent-task";

        await _client.RevokeAsync(taskId);

        var state = await _backend.GetStateAsync(taskId);
        Assert.Equal(TaskState.Revoked, state);
    }

    [Fact]
    public async Task RevokeAsync_MultipleTasks_MarksAllAsRevoked()
    {
        var taskIds = new[] { "task-1", "task-2", "task-3" };
        await _backend.UpdateStateAsync(taskIds[0], TaskState.Pending);
        await _backend.UpdateStateAsync(taskIds[1], TaskState.Started);
        await _backend.StoreResultAsync(
            new TaskResult
            {
                TaskId = taskIds[2],
                State = TaskState.Success,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.Zero,
            }
        );

        await _client.RevokeAsync(taskIds);

        // Pending and Started should be revoked
        Assert.Equal(TaskState.Revoked, await _backend.GetStateAsync(taskIds[0]));
        Assert.Equal(TaskState.Revoked, await _backend.GetStateAsync(taskIds[1]));
        // Success should remain unchanged
        Assert.Equal(TaskState.Success, await _backend.GetStateAsync(taskIds[2]));
    }

    [Fact]
    public async Task RevokeAsync_WithOptions_PassesOptionsToStore()
    {
        var taskId = "task-with-options";
        await _backend.UpdateStateAsync(taskId, TaskState.Pending);

        var options = new RevokeOptions { Terminate = true, Signal = CancellationSignal.Immediate };

        await _client.RevokeAsync(taskId, options);

        var storedOptions = _revocationStore.GetOptions(taskId);
        Assert.NotNull(storedOptions);
        Assert.True(storedOptions.Terminate);
        Assert.Equal(CancellationSignal.Immediate, storedOptions.Signal);
    }

    [Fact]
    public async Task RevokeAsync_WithoutRevocationStore_ThrowsInvalidOperationException()
    {
        var clientWithoutRevocation = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            Options.Create(new CeleryClientOptions()),
            NullLogger<CeleryClient>.Instance,
            revocationStore: null
        );

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            clientWithoutRevocation.RevokeAsync("any-task").AsTask()
        );
    }

    [Fact]
    public async Task IsRevokedAsync_WithoutRevocationStore_ReturnsFalse()
    {
        var clientWithoutRevocation = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            Options.Create(new CeleryClientOptions()),
            NullLogger<CeleryClient>.Instance,
            revocationStore: null
        );

        var isRevoked = await clientWithoutRevocation.IsRevokedAsync("any-task");

        Assert.False(isRevoked);
    }

    [Fact]
    public async Task IsRevokedAsync_RevokedTask_ReturnsTrue()
    {
        var taskId = "revoked-task";
        await _revocationStore.RevokeAsync(taskId);

        var isRevoked = await _client.IsRevokedAsync(taskId);

        Assert.True(isRevoked);
    }

    [Fact]
    public async Task IsRevokedAsync_NonRevokedTask_ReturnsFalse()
    {
        var isRevoked = await _client.IsRevokedAsync("non-revoked-task");

        Assert.False(isRevoked);
    }

    public async ValueTask DisposeAsync()
    {
        await _broker.DisposeAsync();
        await _backend.DisposeAsync();
        await _revocationStore.DisposeAsync();
    }
}
