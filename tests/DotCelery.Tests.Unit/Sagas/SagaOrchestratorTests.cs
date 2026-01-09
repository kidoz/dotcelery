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
/// Tests for <see cref="SagaOrchestrator"/>.
/// </summary>
public sealed class SagaOrchestratorTests : IAsyncDisposable
{
    private readonly InMemorySagaStore _sagaStore = new();
    private readonly InMemoryBroker _broker = new();
    private readonly InMemoryResultBackend _backend = new();
    private readonly JsonMessageSerializer _serializer = new();
    private readonly TestSignalDispatcher _signalDispatcher = new();
    private readonly SagaOrchestrator _orchestrator;

    public SagaOrchestratorTests()
    {
        var options = Options.Create(new SagaOptions { DispatchSignals = true });
        _orchestrator = new SagaOrchestrator(
            _sagaStore,
            _broker,
            _serializer,
            _backend,
            options,
            NullLogger<SagaOrchestrator>.Instance,
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
    public async Task StartAsync_CreatesSagaAndStartsFirstStep()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);

        // Act
        var started = await _orchestrator.StartAsync(saga);

        // Assert
        Assert.NotNull(started);
        Assert.Equal(SagaState.Executing, started.State);
        Assert.NotNull(started.StartedAt);
        Assert.Equal(0, started.CurrentStepIndex);

        // Verify task was published
        Assert.Equal(1, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task StartAsync_GeneratesIdIfNotProvided()
    {
        // Arrange
        var saga = CreateSaga("", 2);

        // Act
        var started = await _orchestrator.StartAsync(saga);

        // Assert
        Assert.NotEmpty(started.Id);
    }

    [Fact]
    public async Task StartAsync_DispatchesStateChangedSignal()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);

        // Act
        await _orchestrator.StartAsync(saga);

        // Assert
        var signal = _signalDispatcher.ReceivedSignals.OfType<SagaStateChangedSignal>().Single();
        Assert.Equal(SagaState.Created, signal.OldState);
        Assert.Equal(SagaState.Executing, signal.NewState);
    }

    [Fact]
    public async Task ContinueAsync_AdvancesToNextStep()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 3);
        await _orchestrator.StartAsync(saga);

        // Complete first step
        var storedSaga = await _sagaStore.GetAsync("saga-1");
        await _sagaStore.UpdateStepStateAsync(
            "saga-1",
            storedSaga!.Steps[0].Id,
            SagaStepState.Completed
        );

        // Act
        var continued = await _orchestrator.ContinueAsync("saga-1");

        // Assert
        Assert.NotNull(continued);
        Assert.Equal(1, continued.CurrentStepIndex);

        // Verify second task was published
        Assert.Equal(2, _broker.GetQueueLength("celery"));
    }

    [Fact]
    public async Task ContinueAsync_WhenAllStepsComplete_CompleteSaga()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        var started = await _orchestrator.StartAsync(saga);

        // Complete both steps
        await _sagaStore.UpdateStepStateAsync(
            "saga-1",
            started.Steps[0].Id,
            SagaStepState.Completed
        );
        await _orchestrator.ContinueAsync("saga-1");
        await _sagaStore.UpdateStepStateAsync(
            "saga-1",
            started.Steps[1].Id,
            SagaStepState.Completed
        );

        // Act
        var final = await _orchestrator.ContinueAsync("saga-1");

        // Assert
        Assert.NotNull(final);
        Assert.Equal(SagaState.Completed, final.State);
        Assert.True(final.IsFinished);
    }

    [Fact]
    public async Task ContinueAsync_WithNonExistentSaga_ReturnsNull()
    {
        // Act
        var result = await _orchestrator.ContinueAsync("nonexistent");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task CompensateAsync_StartsCompensationProcess()
    {
        // Arrange
        var saga = CreateSagaWithCompensation("saga-1", 2);
        var started = await _orchestrator.StartAsync(saga);

        // Complete first step
        await _sagaStore.UpdateStepStateAsync(
            "saga-1",
            started.Steps[0].Id,
            SagaStepState.Completed
        );

        // Act
        await _orchestrator.CompensateAsync("saga-1", "Test failure");

        // Assert
        var updated = await _sagaStore.GetAsync("saga-1");
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Compensating, updated.State);
    }

    [Fact]
    public async Task CompensateAsync_DispatchesCompensationSignals()
    {
        // Arrange
        var saga = CreateSagaWithCompensation("saga-1", 2);
        var started = await _orchestrator.StartAsync(saga);
        await _sagaStore.UpdateStepStateAsync(
            "saga-1",
            started.Steps[0].Id,
            SagaStepState.Completed
        );

        // Clear previous signals
        _signalDispatcher.ReceivedSignals.Clear();

        // Act
        await _orchestrator.CompensateAsync("saga-1", "Test failure");

        // Assert
        Assert.Contains(_signalDispatcher.ReceivedSignals, s => s is SagaCompensationStartedSignal);
        Assert.Contains(_signalDispatcher.ReceivedSignals, s => s is SagaStateChangedSignal);
    }

    [Fact]
    public async Task CompensateAsync_WhenAlreadyCompensating_DoesNothing()
    {
        // Arrange
        var saga = CreateSagaWithCompensation("saga-1", 2);
        var started = await _orchestrator.StartAsync(saga);
        await _sagaStore.UpdateStepStateAsync(
            "saga-1",
            started.Steps[0].Id,
            SagaStepState.Completed
        );
        await _orchestrator.CompensateAsync("saga-1", "First compensation");

        _signalDispatcher.ReceivedSignals.Clear();

        // Act
        await _orchestrator.CompensateAsync("saga-1", "Second compensation");

        // Assert - no new signals dispatched
        Assert.Empty(_signalDispatcher.ReceivedSignals);
    }

    [Fact]
    public async Task GetAsync_ReturnsStoredSaga()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _orchestrator.StartAsync(saga);

        // Act
        var retrieved = await _orchestrator.GetAsync("saga-1");

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal("saga-1", retrieved.Id);
    }

    [Fact]
    public async Task CancelAsync_WithNoCompletedSteps_MarksCancelled()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _orchestrator.StartAsync(saga);

        // Act
        await _orchestrator.CancelAsync("saga-1", "User cancelled");

        // Assert
        var updated = await _sagaStore.GetAsync("saga-1");
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Cancelled, updated.State);
    }

    [Fact]
    public async Task CancelAsync_WithCompletedSteps_StartsCompensation()
    {
        // Arrange
        var saga = CreateSagaWithCompensation("saga-1", 2);
        var started = await _orchestrator.StartAsync(saga);

        // Complete first step
        await _sagaStore.UpdateStepStateAsync(
            "saga-1",
            started.Steps[0].Id,
            SagaStepState.Completed
        );

        // Act
        await _orchestrator.CancelAsync("saga-1", "User cancelled");

        // Assert
        var updated = await _sagaStore.GetAsync("saga-1");
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Compensating, updated.State);
    }

    [Fact]
    public async Task RetryAsync_ResumesFromFailedStep()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _orchestrator.StartAsync(saga);
        await _sagaStore.UpdateStateAsync("saga-1", SagaState.Failed);

        var initialQueueLength = _broker.GetQueueLength("celery");

        // Act
        await _orchestrator.RetryAsync("saga-1");

        // Assert
        var updated = await _sagaStore.GetAsync("saga-1");
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Executing, updated.State);

        // Verify new task was published
        Assert.True(_broker.GetQueueLength("celery") > initialQueueLength);
    }

    [Fact]
    public async Task RetryAsync_WithNonFailedState_DoesNothing()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _orchestrator.StartAsync(saga);

        var initialQueueLength = _broker.GetQueueLength("celery");
        _signalDispatcher.ReceivedSignals.Clear();

        // Act
        await _orchestrator.RetryAsync("saga-1");

        // Assert - no change
        Assert.Equal(initialQueueLength, _broker.GetQueueLength("celery"));
    }

    private static Saga CreateSaga(string id, int stepCount) =>
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
                })
                .ToList(),
        };

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
