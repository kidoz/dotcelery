namespace DotCelery.Tests.Unit.Client;

using DotCelery.Backend.InMemory.Storage;
using DotCelery.Broker.InMemory;
using DotCelery.Client;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

public class CeleryClientRevocationTests : IAsyncDisposable
{
    private readonly InMemoryBroker _broker = new();
    private readonly ResultBackend _backend = new(new InMemoryStorageProvider());
    private readonly RevocationStore _revocationStore = new(new InMemoryStorageProvider());
    private readonly JsonMessageSerializer _serializer = new();
    private readonly CeleryClient _client;
    private readonly DelayedMessageStore _delayed = new(new InMemoryStorageProvider());

    public CeleryClientRevocationTests()
    {
        var options = Options.Create(new CeleryClientOptions());
        _client = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            options,
            NullLogger<CeleryClient>.Instance,
            _revocationStore,
            delayedMessageStore: _delayed
        );
    }

    [Fact]
    public async Task CancelScheduledAsync_ClaimedMessage_IsRevokedBeforeItsCopyCanExecute()
    {
        var message = new TaskMessage
        {
            Id = "claimed",
            Task = CancelledTask.TaskName,
            Args = [],
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
        };
        await _delayed.AddAsync(message, DateTimeOffset.UtcNow.AddMinutes(-1));
        await using var due = _delayed
            .GetDueMessagesAsync(DateTimeOffset.UtcNow)
            .GetAsyncEnumerator();
        Assert.True(await due.MoveNextAsync());
        var heldCopy = due.Current;

        Assert.True(await _client.CancelScheduledAsync(message.Id));
        Assert.True(await _revocationStore.IsRevokedAsync(message.Id));
        var result = new AsyncResult(message.Id, _client);
        Assert.True(await result.IsCompleteAsync());
        Assert.Equal(TaskState.Revoked, (await result.WaitAsync(TimeSpan.FromSeconds(1))).State);

        await using var services = new ServiceCollection()
            .AddTransient<CancelledTask>()
            .BuildServiceProvider();
        var registry = new TaskRegistry();
        registry.Register(typeof(CancelledTask), CancelledTask.TaskName);
        var options = Options.Create(new WorkerOptions());
        using var revocations = new RevocationManager(
            options,
            NullLogger<RevocationManager>.Instance,
            _revocationStore
        );
        var executor = new TaskExecutor(
            registry,
            services,
            _serializer,
            _backend,
            revocations,
            new TaskFilterPipeline(
                services,
                Options.Create(new TaskFilterOptions()),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance
        );
        var outcome = await executor.ExecuteAsync(
            new BrokerMessage
            {
                Message = heldCopy,
                DeliveryTag = "copy",
                Queue = "celery",
                ReceivedAt = DateTimeOffset.UtcNow,
            },
            "worker",
            CancellationToken.None
        );
        Assert.Equal(TaskState.Revoked, outcome.State);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancelScheduledAsync_WithoutDurableRevocation_DoesNotRemoveTheTask(
        bool failingStore
    )
    {
        await _delayed.AddAsync(
            new TaskMessage
            {
                Id = "pending",
                Task = CancelledTask.TaskName,
                Args = [],
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
            },
            DateTimeOffset.UtcNow.AddHours(1)
        );
        IRevocationStore? revocations = null;
        if (failingStore)
        {
            revocations = Substitute.For<IRevocationStore>();
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
            revocations
                .RevokeAsync(
                    Arg.Any<string>(),
                    Arg.Any<RevokeOptions?>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(_ =>
                    ValueTask.FromException(new InvalidOperationException("Revocation unavailable"))
                );
#pragma warning restore CA2012
        }
        var client = new CeleryClient(
            _broker,
            _backend,
            _serializer,
            Options.Create(new CeleryClientOptions()),
            NullLogger<CeleryClient>.Instance,
            revocations,
            delayedMessageStore: _delayed
        );

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.CancelScheduledAsync("pending").AsTask()
        );

        Assert.Equal(1, await _delayed.GetPendingCountAsync());
        Assert.Null(await _backend.GetStateAsync("pending"));
    }

    private sealed class CancelledTask : ITask
    {
        public static string TaskName => "tests.cancelled";

        public Task ExecuteAsync(
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => throw new InvalidOperationException($"{GetType().Name} must never run.");
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
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var revocations = _revocationStore
            .SubscribeAsync(cts.Token)
            .GetAsyncEnumerator(cts.Token);
        var next = revocations.MoveNextAsync();

        await _client.RevokeAsync(taskId, options);

        Assert.True(await next);
        var storedOptions = revocations.Current.Options;
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
