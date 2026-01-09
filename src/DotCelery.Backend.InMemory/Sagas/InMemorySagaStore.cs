using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DotCelery.Core.Sagas;

namespace DotCelery.Backend.InMemory.Sagas;

/// <summary>
/// In-memory implementation of <see cref="ISagaStore"/> for testing.
/// </summary>
public sealed class InMemorySagaStore : ISagaStore
{
    private readonly ConcurrentDictionary<string, Saga> _sagas = new();
    private readonly ConcurrentDictionary<string, string> _taskToSaga = new();

    /// <inheritdoc />
    public ValueTask CreateAsync(Saga saga, CancellationToken cancellationToken = default)
    {
        _sagas[saga.Id] = saga;

        // Index task IDs from all steps
        foreach (var step in saga.Steps)
        {
            if (step.ExecuteTaskId is not null)
            {
                _taskToSaga[step.ExecuteTaskId] = saga.Id;
            }

            if (step.CompensateTaskId is not null)
            {
                _taskToSaga[step.CompensateTaskId] = saga.Id;
            }
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<Saga?> GetAsync(string sagaId, CancellationToken cancellationToken = default)
    {
        _sagas.TryGetValue(sagaId, out var saga);
        return ValueTask.FromResult(saga);
    }

    /// <inheritdoc />
    public ValueTask<Saga?> UpdateStateAsync(
        string sagaId,
        SagaState state,
        string? failureReason = null,
        CancellationToken cancellationToken = default
    )
    {
        if (!_sagas.TryGetValue(sagaId, out var saga))
        {
            return ValueTask.FromResult<Saga?>(null);
        }

        var isTerminal =
            state
            is SagaState.Completed
                or SagaState.Failed
                or SagaState.Compensated
                or SagaState.CompensationFailed
                or SagaState.Cancelled;

        var updatedSaga = saga with
        {
            State = state,
            FailureReason = failureReason ?? saga.FailureReason,
            CompletedAt = isTerminal ? DateTimeOffset.UtcNow : saga.CompletedAt,
        };

        _sagas[sagaId] = updatedSaga;
        return ValueTask.FromResult<Saga?>(updatedSaga);
    }

    /// <inheritdoc />
    public ValueTask<Saga?> UpdateStepStateAsync(
        string sagaId,
        string stepId,
        SagaStepState state,
        string? taskId = null,
        string? compensateTaskId = null,
        object? result = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default
    )
    {
        if (!_sagas.TryGetValue(sagaId, out var saga))
        {
            return ValueTask.FromResult<Saga?>(null);
        }

        var now = DateTimeOffset.UtcNow;
        var updatedSteps = saga
            .Steps.Select(s =>
            {
                if (s.Id != stepId)
                {
                    return s;
                }

                return s with
                {
                    State = state,
                    ExecuteTaskId = taskId ?? s.ExecuteTaskId,
                    CompensateTaskId = compensateTaskId ?? s.CompensateTaskId,
                    Result = result ?? s.Result,
                    Error = errorMessage ?? s.Error,
                    StartedAt = state == SagaStepState.Executing ? now : s.StartedAt,
                    CompletedAt = state is SagaStepState.Completed or SagaStepState.Failed
                        ? now
                        : s.CompletedAt,
                };
            })
            .ToList();

        var updatedSaga = saga with { Steps = updatedSteps };

        // Index new task IDs
        if (taskId is not null)
        {
            _taskToSaga[taskId] = sagaId;
        }

        if (compensateTaskId is not null)
        {
            _taskToSaga[compensateTaskId] = sagaId;
        }

        // Auto-transition saga state based on step states
        updatedSaga = TransitionSagaState(updatedSaga, state, errorMessage);

        _sagas[sagaId] = updatedSaga;
        return ValueTask.FromResult<Saga?>(updatedSaga);
    }

    /// <inheritdoc />
    public ValueTask<Saga?> MarkStepCompensatedAsync(
        string sagaId,
        string stepId,
        bool success,
        string? compensateTaskId = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default
    )
    {
        if (!_sagas.TryGetValue(sagaId, out var saga))
        {
            return ValueTask.FromResult<Saga?>(null);
        }

        var updatedSteps = saga
            .Steps.Select(s =>
            {
                if (s.Id != stepId)
                {
                    return s;
                }

                return s with
                {
                    State = success ? SagaStepState.Compensated : SagaStepState.CompensationFailed,
                    CompensateTaskId = compensateTaskId ?? s.CompensateTaskId,
                    Error = errorMessage ?? s.Error,
                };
            })
            .ToList();

        var updatedSaga = saga with { Steps = updatedSteps };

        // Index compensation task ID
        if (compensateTaskId is not null)
        {
            _taskToSaga[compensateTaskId] = sagaId;
        }

        // Check if all compensation is done
        var stepsNeedingCompensation = updatedSaga
            .Steps.Where(s =>
                s.RequiresCompensation
                && (s.State == SagaStepState.Completed || s.State == SagaStepState.Compensating)
            )
            .ToList();

        if (stepsNeedingCompensation.Count == 0)
        {
            // All compensable steps are now compensated or failed compensation
            var anyCompensationFailed = updatedSaga.Steps.Any(s =>
                s.State == SagaStepState.CompensationFailed
            );
            updatedSaga = updatedSaga with
            {
                State = anyCompensationFailed
                    ? SagaState.CompensationFailed
                    : SagaState.Compensated,
                CompletedAt = DateTimeOffset.UtcNow,
            };
        }

        _sagas[sagaId] = updatedSaga;
        return ValueTask.FromResult<Saga?>(updatedSaga);
    }

    /// <inheritdoc />
    public ValueTask<Saga?> AdvanceStepAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        if (!_sagas.TryGetValue(sagaId, out var saga))
        {
            return ValueTask.FromResult<Saga?>(null);
        }

        var nextIndex = saga.CurrentStepIndex + 1;
        var updatedSaga = saga with { CurrentStepIndex = nextIndex };

        // Check if all steps completed
        if (nextIndex >= saga.TotalSteps)
        {
            updatedSaga = updatedSaga with
            {
                State = SagaState.Completed,
                CompletedAt = DateTimeOffset.UtcNow,
            };
        }

        _sagas[sagaId] = updatedSaga;
        return ValueTask.FromResult<Saga?>(updatedSaga);
    }

    /// <inheritdoc />
    public ValueTask<bool> DeleteAsync(string sagaId, CancellationToken cancellationToken = default)
    {
        if (!_sagas.TryRemove(sagaId, out var saga))
        {
            return ValueTask.FromResult(false);
        }

        // Remove task mappings
        foreach (var step in saga.Steps)
        {
            if (step.ExecuteTaskId is not null)
            {
                _taskToSaga.TryRemove(step.ExecuteTaskId, out _);
            }

            if (step.CompensateTaskId is not null)
            {
                _taskToSaga.TryRemove(step.CompensateTaskId, out _);
            }
        }

        return ValueTask.FromResult(true);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetSagaIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        _taskToSaga.TryGetValue(taskId, out var sagaId);
        return ValueTask.FromResult(sagaId);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<Saga> GetByStateAsync(
        SagaState state,
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        var sagas = _sagas
            .Values.Where(s => s.State == state)
            .OrderBy(s => s.CreatedAt)
            .Take(limit)
            .ToList();

        foreach (var saga in sagas)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            yield return saga;
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _sagas.Clear();
        _taskToSaga.Clear();
        return ValueTask.CompletedTask;
    }

    private static Saga TransitionSagaState(
        Saga saga,
        SagaStepState stepState,
        string? errorMessage
    )
    {
        // If step failed, transition saga to Compensating or Failed
        if (stepState == SagaStepState.Failed)
        {
            var hasCompensableSteps = saga
                .Steps.Take(saga.CurrentStepIndex + 1)
                .Any(s => s.State == SagaStepState.Completed && s.RequiresCompensation);

            return saga with
            {
                State = hasCompensableSteps ? SagaState.Compensating : SagaState.Failed,
                FailureReason = errorMessage ?? saga.FailureReason,
            };
        }

        return saga;
    }
}
