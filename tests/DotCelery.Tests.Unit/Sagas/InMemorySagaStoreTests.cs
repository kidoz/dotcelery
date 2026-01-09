using DotCelery.Backend.InMemory.Sagas;
using DotCelery.Core.Canvas;
using DotCelery.Core.Sagas;

namespace DotCelery.Tests.Unit.Sagas;

/// <summary>
/// Tests for <see cref="InMemorySagaStore"/>.
/// </summary>
public sealed class InMemorySagaStoreTests : IAsyncDisposable
{
    private readonly InMemorySagaStore _store = new();

    public async ValueTask DisposeAsync()
    {
        await _store.DisposeAsync();
    }

    [Fact]
    public async Task CreateAsync_StoresSaga()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 3);

        // Act
        await _store.CreateAsync(saga);
        var retrieved = await _store.GetAsync("saga-1");

        // Assert
        Assert.NotNull(retrieved);
        Assert.Equal("saga-1", retrieved.Id);
        Assert.Equal(3, retrieved.TotalSteps);
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
    public async Task GetSagaIdForTaskAsync_ReturnsCorrectSagaId()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        saga = saga with
        {
            Steps =
            [
                saga.Steps[0] with
                {
                    ExecuteTaskId = "task-1",
                },
                saga.Steps[1] with
                {
                    ExecuteTaskId = "task-2",
                },
            ],
        };
        await _store.CreateAsync(saga);

        // Act
        var sagaId1 = await _store.GetSagaIdForTaskAsync("task-1");
        var sagaId2 = await _store.GetSagaIdForTaskAsync("task-2");
        var sagaId3 = await _store.GetSagaIdForTaskAsync("task-3");

        // Assert
        Assert.Equal("saga-1", sagaId1);
        Assert.Equal("saga-1", sagaId2);
        Assert.Null(sagaId3);
    }

    [Fact]
    public async Task UpdateStateAsync_UpdatesSagaState()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _store.CreateAsync(saga);

        // Act
        var updated = await _store.UpdateStateAsync(
            "saga-1",
            SagaState.Compensating,
            "Test failure"
        );
        var retrieved = await _store.GetAsync("saga-1");

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Compensating, updated.State);
        Assert.Equal("Test failure", updated.FailureReason);
        Assert.NotNull(retrieved);
        Assert.Equal(SagaState.Compensating, retrieved.State);
    }

    [Fact]
    public async Task UpdateStateAsync_ToTerminalState_SetsCompletedAt()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _store.CreateAsync(saga);

        // Act
        var updated = await _store.UpdateStateAsync("saga-1", SagaState.Completed);

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Completed, updated.State);
        Assert.NotNull(updated.CompletedAt);
    }

    [Fact]
    public async Task UpdateStepStateAsync_UpdatesStepState()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _store.CreateAsync(saga);
        var stepId = saga.Steps[0].Id;

        // Act
        var updated = await _store.UpdateStepStateAsync(
            "saga-1",
            stepId,
            SagaStepState.Executing,
            taskId: "task-1"
        );

        // Assert
        Assert.NotNull(updated);
        var step = updated.Steps.First(s => s.Id == stepId);
        Assert.Equal(SagaStepState.Executing, step.State);
        Assert.Equal("task-1", step.ExecuteTaskId);
        Assert.NotNull(step.StartedAt);
    }

    [Fact]
    public async Task UpdateStepStateAsync_ToCompleted_SetsCompletedAt()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _store.CreateAsync(saga);
        var stepId = saga.Steps[0].Id;

        // Act
        var updated = await _store.UpdateStepStateAsync(
            "saga-1",
            stepId,
            SagaStepState.Completed,
            result: new { Data = "test" }
        );

        // Assert
        Assert.NotNull(updated);
        var step = updated.Steps.First(s => s.Id == stepId);
        Assert.Equal(SagaStepState.Completed, step.State);
        Assert.NotNull(step.CompletedAt);
        Assert.NotNull(step.Result);
    }

    [Fact]
    public async Task UpdateStepStateAsync_IndexesTaskId()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _store.CreateAsync(saga);
        var stepId = saga.Steps[0].Id;

        // Act
        await _store.UpdateStepStateAsync(
            "saga-1",
            stepId,
            SagaStepState.Executing,
            taskId: "new-task-id"
        );

        var sagaId = await _store.GetSagaIdForTaskAsync("new-task-id");

        // Assert
        Assert.Equal("saga-1", sagaId);
    }

    [Fact]
    public async Task MarkStepCompensatedAsync_MarksStepAsCompensated()
    {
        // Arrange
        var saga = CreateSagaWithCompensation("saga-1", 2);
        await _store.CreateAsync(saga);
        var stepId = saga.Steps[0].Id;

        // First complete the step, then mark it for compensation
        await _store.UpdateStepStateAsync("saga-1", stepId, SagaStepState.Completed);
        await _store.UpdateStateAsync("saga-1", SagaState.Compensating);

        // Act
        var updated = await _store.MarkStepCompensatedAsync(
            "saga-1",
            stepId,
            success: true,
            compensateTaskId: "compensate-task-1"
        );

        // Assert
        Assert.NotNull(updated);
        var step = updated.Steps.First(s => s.Id == stepId);
        Assert.Equal(SagaStepState.Compensated, step.State);
        Assert.Equal("compensate-task-1", step.CompensateTaskId);
    }

    [Fact]
    public async Task MarkStepCompensatedAsync_WithFailure_SetsError()
    {
        // Arrange
        var saga = CreateSagaWithCompensation("saga-1", 2);
        await _store.CreateAsync(saga);
        var stepId = saga.Steps[0].Id;
        await _store.UpdateStepStateAsync("saga-1", stepId, SagaStepState.Completed);

        // Act
        var updated = await _store.MarkStepCompensatedAsync(
            "saga-1",
            stepId,
            success: false,
            errorMessage: "Compensation failed"
        );

        // Assert
        Assert.NotNull(updated);
        var step = updated.Steps.First(s => s.Id == stepId);
        Assert.Equal(SagaStepState.CompensationFailed, step.State);
        Assert.Equal("Compensation failed", step.Error);
    }

    [Fact]
    public async Task AdvanceStepAsync_IncrementsCurrentStepIndex()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 3);
        await _store.CreateAsync(saga);

        // Act
        var updated = await _store.AdvanceStepAsync("saga-1");

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(1, updated.CurrentStepIndex);
    }

    [Fact]
    public async Task AdvanceStepAsync_PastLastStep_CompleteSaga()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2) with
        {
            CurrentStepIndex = 1,
        };
        await _store.CreateAsync(saga);

        // Act
        var updated = await _store.AdvanceStepAsync("saga-1");

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(SagaState.Completed, updated.State);
        Assert.NotNull(updated.CompletedAt);
    }

    [Fact]
    public async Task DeleteAsync_RemovesSaga()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        await _store.CreateAsync(saga);

        // Act
        var deleted = await _store.DeleteAsync("saga-1");
        var retrieved = await _store.GetAsync("saga-1");

        // Assert
        Assert.True(deleted);
        Assert.Null(retrieved);
    }

    [Fact]
    public async Task DeleteAsync_RemovesTaskMappings()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 2);
        saga = saga with
        {
            Steps =
            [
                saga.Steps[0] with
                {
                    ExecuteTaskId = "task-1",
                },
                saga.Steps[1] with
                {
                    ExecuteTaskId = "task-2",
                },
            ],
        };
        await _store.CreateAsync(saga);

        // Act
        await _store.DeleteAsync("saga-1");
        var sagaId = await _store.GetSagaIdForTaskAsync("task-1");

        // Assert
        Assert.Null(sagaId);
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
    public async Task GetByStateAsync_ReturnsMatchingSagas()
    {
        // Arrange
        var saga1 = CreateSaga("saga-1", 2) with
        {
            State = SagaState.Executing,
        };
        var saga2 = CreateSaga("saga-2", 2) with { State = SagaState.Executing };
        var saga3 = CreateSaga("saga-3", 2) with { State = SagaState.Completed };
        await _store.CreateAsync(saga1);
        await _store.CreateAsync(saga2);
        await _store.CreateAsync(saga3);

        // Act
        var executingSagas = await _store.GetByStateAsync(SagaState.Executing).ToListAsync();

        // Assert
        Assert.Equal(2, executingSagas.Count);
        Assert.All(executingSagas, s => Assert.Equal(SagaState.Executing, s.State));
    }

    [Fact]
    public async Task Saga_Progress_CalculatesCorrectly()
    {
        // Arrange
        var saga = CreateSaga("saga-1", 4);
        await _store.CreateAsync(saga);

        // Act - Complete 2 of 4 steps
        await _store.UpdateStepStateAsync("saga-1", saga.Steps[0].Id, SagaStepState.Completed);
        var updated = await _store.UpdateStepStateAsync(
            "saga-1",
            saga.Steps[1].Id,
            SagaStepState.Completed
        );

        // Assert
        Assert.NotNull(updated);
        Assert.Equal(50.0, updated.Progress);
        Assert.Equal(2, updated.CompletedSteps);
    }

    private static Saga CreateSaga(string id, int stepCount) =>
        new()
        {
            Id = id,
            Name = $"Test Saga {id}",
            State = SagaState.Executing,
            CreatedAt = DateTimeOffset.UtcNow,
            Steps = Enumerable
                .Range(0, stepCount)
                .Select(i => new SagaStep
                {
                    Id = $"step-{i}",
                    Name = $"Step {i}",
                    Order = i,
                    ExecuteTask = new Signature { TaskName = $"task.{i}" },
                })
                .ToList(),
        };

    private static Saga CreateSagaWithCompensation(string id, int stepCount) =>
        new()
        {
            Id = id,
            Name = $"Test Saga {id}",
            State = SagaState.Executing,
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
}
