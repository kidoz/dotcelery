using DotCelery.Backend.InMemory;
using DotCelery.Backend.InMemory.Sagas;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Canvas;
using DotCelery.Core.Sagas;
using DotCelery.Core.Serialization;
using DotCelery.Core.Signals;
using DotCelery.Worker.Sagas;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Sagas;

/// <summary>
/// Tests for <see cref="SagaCompletionHandler"/>.
/// </summary>
public sealed class SagaCompletionHandlerTests : IAsyncDisposable
{
    private readonly InMemorySagaStore _sagaStore = new();
    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryResultBackend _backend = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly TestSignalDispatcher _signalDispatcher = new();
    private readonly SagaOrchestrator _orchestrator;
    private readonly SagaCompletionHandler _handler;

    public SagaCompletionHandlerTests()
    {
        var options = Options.Create(
            new SagaOptions { DispatchSignals = true, AutoCompensateOnFailure = true }
        );

        _orchestrator = new SagaOrchestrator(
            _sagaStore,
            _broker,
            _serializer,
            _backend,
            options,
            NullLogger<SagaOrchestrator>.Instance,
            _signalDispatcher
        );

        _handler = new SagaCompletionHandler(
            _sagaStore,
            _orchestrator,
            options,
            NullLogger<SagaCompletionHandler>.Instance,
            _signalDispatcher
        );
    }

    public async ValueTask DisposeAsync()
    {
        await _sagaStore.DisposeAsync();
        await _broker.DisposeAsync();
        await _backend.DisposeAsync();
    }

    [Fact]
    public async Task HandleAsync_TaskSuccessSignal_UpdatesStepState()
    {
        // Arrange
        var saga = await SetupSagaWithExecutingStep();
        var signal = new TaskSuccessSignal
        {
            TaskId = "task-0",
            TaskName = "task.execute.0",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
            Result = "success",
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert
        var updated = await _sagaStore.GetAsync(saga.Id);
        Assert.NotNull(updated);
        var step = updated.Steps[0];
        Assert.Equal(SagaStepState.Completed, step.State);
    }

    [Fact]
    public async Task HandleAsync_TaskSuccessSignal_ContinuesToNextStep()
    {
        // Arrange
        var saga = await SetupSagaWithExecutingStep();
        var initialQueueLength = _broker.GetQueueLength("celery");

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-0",
            TaskName = "task.execute.0",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert - next task should be published
        Assert.True(_broker.GetQueueLength("celery") > initialQueueLength);
    }

    [Fact]
    public async Task HandleAsync_TaskSuccessSignal_DispatchesStepCompletedSignal()
    {
        // Arrange
        var saga = await SetupSagaWithExecutingStep();
        _signalDispatcher.ReceivedSignals.Clear();

        var signal = new TaskSuccessSignal
        {
            TaskId = "task-0",
            TaskName = "task.execute.0",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert
        Assert.Contains(_signalDispatcher.ReceivedSignals, s => s is SagaStepCompletedSignal);
    }

    [Fact]
    public async Task HandleAsync_TaskSuccessSignal_ForNonSagaTask_DoesNothing()
    {
        // Arrange - no saga setup
        var signal = new TaskSuccessSignal
        {
            TaskId = "random-task",
            TaskName = "some.task",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act & Assert - should not throw
        await _handler.HandleAsync(signal, CancellationToken.None);
    }

    [Fact]
    public async Task HandleAsync_TaskFailureSignal_UpdatesStepToFailed()
    {
        // Arrange
        var saga = await SetupSagaWithExecutingStep();
        var signal = new TaskFailureSignal
        {
            TaskId = "task-0",
            TaskName = "task.execute.0",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
            Exception = new InvalidOperationException("Test failure"),
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert
        var updated = await _sagaStore.GetAsync(saga.Id);
        Assert.NotNull(updated);
        var step = updated.Steps[0];
        Assert.Equal(SagaStepState.Failed, step.State);
    }

    [Fact]
    public async Task HandleAsync_TaskFailureSignal_WithCompensableSteps_TriggersCompensation()
    {
        // Arrange
        var saga = await SetupSagaWithCompletedFirstStep();

        // Fail the second step
        var signal = new TaskFailureSignal
        {
            TaskId = "task-1",
            TaskName = "task.execute.1",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
            Exception = new InvalidOperationException("Test failure"),
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert - saga should be in compensating state
        var updated = await _sagaStore.GetAsync(saga.Id);
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Compensating, updated.State);
    }

    [Fact]
    public async Task HandleAsync_TaskRevokedSignal_UpdatesStepToFailed()
    {
        // Arrange
        var saga = await SetupSagaWithExecutingStep();
        var signal = new TaskRevokedSignal
        {
            TaskId = "task-0",
            TaskName = "task.execute.0",
            Timestamp = DateTimeOffset.UtcNow,
            Terminated = true,
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert
        var updated = await _sagaStore.GetAsync(saga.Id);
        Assert.NotNull(updated);
        var step = updated.Steps[0];
        Assert.Equal(SagaStepState.Failed, step.State);
    }

    [Fact]
    public async Task HandleAsync_TaskRejectedSignal_UpdatesStepToFailed()
    {
        // Arrange
        var saga = await SetupSagaWithExecutingStep();
        var signal = new TaskRejectedSignal
        {
            TaskId = "task-0",
            TaskName = "task.execute.0",
            Timestamp = DateTimeOffset.UtcNow,
            Reason = "Queue full",
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert
        var updated = await _sagaStore.GetAsync(saga.Id);
        Assert.NotNull(updated);
        var step = updated.Steps[0];
        Assert.Equal(SagaStepState.Failed, step.State);
    }

    [Fact]
    public async Task HandleAsync_CompensationTaskSuccess_MarksStepCompensated()
    {
        // Arrange
        var saga = await SetupSagaInCompensatingState();

        var signal = new TaskSuccessSignal
        {
            TaskId = "compensate-task-0",
            TaskName = "task.compensate.0",
            Timestamp = DateTimeOffset.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
        };

        // Act
        await _handler.HandleAsync(signal, CancellationToken.None);

        // Assert
        var updated = await _sagaStore.GetAsync(saga.Id);
        Assert.NotNull(updated);
        var step = updated.Steps[0];
        Assert.Equal(SagaStepState.Compensated, step.State);
    }

    private async Task<Saga> SetupSagaWithExecutingStep()
    {
        var saga = CreateSagaWithCompensation("saga-1", 2) with { State = SagaState.Executing };
        await _sagaStore.CreateAsync(saga);

        // Simulate orchestrator starting the saga - set first step to executing with task ID
        await _sagaStore.UpdateStepStateAsync(
            saga.Id,
            saga.Steps[0].Id,
            SagaStepState.Executing,
            taskId: "task-0"
        );

        return saga;
    }

    private async Task<Saga> SetupSagaWithCompletedFirstStep()
    {
        var saga = CreateSagaWithCompensation("saga-1", 2) with
        {
            State = SagaState.Executing,
            CurrentStepIndex = 1,
        };
        await _sagaStore.CreateAsync(saga);

        // Mark first step as completed
        await _sagaStore.UpdateStepStateAsync(
            saga.Id,
            saga.Steps[0].Id,
            SagaStepState.Completed,
            taskId: "task-0"
        );

        // Mark second step as executing
        await _sagaStore.UpdateStepStateAsync(
            saga.Id,
            saga.Steps[1].Id,
            SagaStepState.Executing,
            taskId: "task-1"
        );

        return saga;
    }

    private async Task<Saga> SetupSagaInCompensatingState()
    {
        var saga = CreateSagaWithCompensation("saga-1", 2) with { State = SagaState.Compensating };
        await _sagaStore.CreateAsync(saga);

        // Mark first step as completed with compensation task ID
        saga = saga with
        {
            Steps =
            [
                saga.Steps[0] with
                {
                    State = SagaStepState.Completed,
                    ExecuteTaskId = "task-0",
                    CompensateTaskId = "compensate-task-0",
                },
                saga.Steps[1],
            ],
        };

        // Update the saga in store with the compensation task ID
        await _sagaStore.UpdateStepStateAsync(
            saga.Id,
            saga.Steps[0].Id,
            SagaStepState.Completed,
            taskId: "task-0"
        );

        // Mark as compensating with compensation task ID
        await _sagaStore.MarkStepCompensatedAsync(
            saga.Id,
            saga.Steps[0].Id,
            success: false,
            compensateTaskId: "compensate-task-0"
        );

        return saga;
    }

    private static Saga CreateSagaWithCompensation(string id, int stepCount) =>
        new()
        {
            Id = id,
            Name = $"Test Saga {id}",
            State = SagaState.Created,
            CreatedAt = DateTimeOffset.UtcNow,
            Steps = Enumerable
                .Range(0, stepCount)
                .Select(i => new SagaStep
                {
                    Id = $"step-{i}",
                    Name = $"Step {i}",
                    Order = i,
                    ExecuteTask = new Signature { TaskName = $"task.execute.{i}" },
                    CompensateTask = new Signature { TaskName = $"task.compensate.{i}" },
                })
                .ToList(),
        };

    private sealed class TestSignalDispatcher : ITaskSignalDispatcher
    {
        public List<ITaskSignal> ReceivedSignals { get; } = [];

        public ValueTask DispatchAsync<TSignal>(
            TSignal signal,
            CancellationToken cancellationToken = default
        )
            where TSignal : ITaskSignal
        {
            ReceivedSignals.Add(signal);
            return ValueTask.CompletedTask;
        }
    }
}
