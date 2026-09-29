using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;
using DotCelery.Core.Sagas;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="ISagaStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// Saga updates are atomic. A failed step moves the saga to compensating when an earlier step
/// needs compensation, and to failed otherwise; a saga is completed when it advances past its
/// last step, and compensated (or compensation failed) when its last compensation finishes.
/// </remarks>
public sealed class SagaStore : ISagaStore
{
    private static readonly JsonTypeInfo<Saga> SagaTypeInfo = StoreJson.TypeInfo<Saga>();

    private readonly IDocumentStore _documents;
    private readonly TimeProvider _timeProvider;
    private readonly string _sagas;
    private readonly string _tasks;

    /// <summary>
    /// Initializes a new instance of the <see cref="SagaStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for step and saga times.</param>
    public SagaStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _documents = storage.Documents;
        _timeProvider = timeProvider ?? TimeProvider.System;
        var storeOptions = options?.Value ?? new StorageStoreOptions();
        _sagas = storeOptions.Name("sagas");
        _tasks = storeOptions.Name("saga-tasks");
    }

    /// <inheritdoc />
    public async ValueTask CreateAsync(Saga saga, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(saga);

        await _documents
            .UpsertAsync(_sagas, saga.Id, saga, SagaTypeInfo, WriteOptions(saga), cancellationToken)
            .ConfigureAwait(false);

        foreach (var step in saga.Steps)
        {
            await IndexTaskAsync(saga.Id, step.ExecuteTaskId, cancellationToken)
                .ConfigureAwait(false);
            await IndexTaskAsync(saga.Id, step.CompensateTaskId, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> GetAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    ) =>
        (
            await _documents
                .GetAsync(_sagas, sagaId, SagaTypeInfo, cancellationToken)
                .ConfigureAwait(false)
        )?.Value;

    /// <inheritdoc />
    public ValueTask<Saga?> UpdateStateAsync(
        string sagaId,
        SagaState state,
        string? failureReason = null,
        CancellationToken cancellationToken = default
    ) =>
        UpdateAsync(
            sagaId,
            saga =>
                saga with
                {
                    State = state,
                    FailureReason = failureReason ?? saga.FailureReason,
                    CompletedAt = IsFinished(state) ? _timeProvider.GetUtcNow() : saga.CompletedAt,
                },
            cancellationToken
        );

    /// <inheritdoc />
    public async ValueTask<Saga?> UpdateStepStateAsync(
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
        var updated = await UpdateAsync(
                sagaId,
                saga =>
                {
                    var now = _timeProvider.GetUtcNow();
                    var steps = UpdateStep(
                        saga,
                        stepId,
                        step =>
                            step with
                            {
                                State = state,
                                ExecuteTaskId = taskId ?? step.ExecuteTaskId,
                                CompensateTaskId = compensateTaskId ?? step.CompensateTaskId,
                                Result = result ?? step.Result,
                                Error = errorMessage ?? step.Error,
                                StartedAt = state == SagaStepState.Executing ? now : step.StartedAt,
                                CompletedAt = state
                                    is SagaStepState.Completed
                                        or SagaStepState.Failed
                                    ? now
                                    : step.CompletedAt,
                            }
                    );

                    return state == SagaStepState.Failed
                        ? Fail(saga with { Steps = steps }, errorMessage)
                        : saga with
                        {
                            Steps = steps,
                        };
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        if (updated is not null)
        {
            await IndexTaskAsync(sagaId, taskId, cancellationToken).ConfigureAwait(false);
            await IndexTaskAsync(sagaId, compensateTaskId, cancellationToken).ConfigureAwait(false);
        }

        return updated;
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> MarkStepCompensatedAsync(
        string sagaId,
        string stepId,
        bool success,
        string? compensateTaskId = null,
        string? errorMessage = null,
        CancellationToken cancellationToken = default
    )
    {
        var updated = await UpdateAsync(
                sagaId,
                saga =>
                {
                    var compensated = saga with
                    {
                        Steps = UpdateStep(
                            saga,
                            stepId,
                            step =>
                                step with
                                {
                                    State = success
                                        ? SagaStepState.Compensated
                                        : SagaStepState.CompensationFailed,
                                    CompensateTaskId = compensateTaskId ?? step.CompensateTaskId,
                                    Error = errorMessage ?? step.Error,
                                }
                        ),
                    };

                    var compensationPending = compensated.Steps.Any(s =>
                        s.RequiresCompensation
                        && s.State is SagaStepState.Completed or SagaStepState.Compensating
                    );
                    if (compensationPending)
                    {
                        return compensated;
                    }

                    return compensated with
                    {
                        State = compensated.Steps.Any(s =>
                            s.State == SagaStepState.CompensationFailed
                        )
                            ? SagaState.CompensationFailed
                            : SagaState.Compensated,
                        CompletedAt = _timeProvider.GetUtcNow(),
                    };
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        if (updated is not null)
        {
            await IndexTaskAsync(sagaId, compensateTaskId, cancellationToken).ConfigureAwait(false);
        }

        return updated;
    }

    /// <inheritdoc />
    public ValueTask<Saga?> AdvanceStepAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    ) =>
        UpdateAsync(
            sagaId,
            saga =>
                saga.CurrentStepIndex + 1 >= saga.TotalSteps
                    ? saga with
                    {
                        CurrentStepIndex = saga.CurrentStepIndex + 1,
                        State = SagaState.Completed,
                        CompletedAt = _timeProvider.GetUtcNow(),
                    }
                    : saga with
                    {
                        CurrentStepIndex = saga.CurrentStepIndex + 1,
                    },
            cancellationToken
        );

    /// <inheritdoc />
    public async ValueTask<bool> DeleteAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        var deleted = await _documents
            .DeleteAsync(_sagas, sagaId, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await _documents
            .DeleteManyAsync(_tasks, new DocumentFilter { IndexKey = sagaId }, cancellationToken)
            .ConfigureAwait(false);

        return deleted;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetSagaIdForTaskAsync(
        string taskId,
        CancellationToken cancellationToken = default
    ) =>
        (
            await _documents.GetAsync(_tasks, taskId, cancellationToken).ConfigureAwait(false)
        )?.IndexKey;

    /// <inheritdoc />
    /// <remarks>Sagas are returned oldest first.</remarks>
    public async IAsyncEnumerable<Saga> GetByStateAsync(
        SagaState state,
        int limit = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        await foreach (
            var document in _documents
                .QueryAsync(
                    _sagas,
                    new DocumentFilter { IndexKey = state.ToString() },
                    SagaTypeInfo,
                    new DocumentPage { Limit = limit },
                    cancellationToken
                )
                .ConfigureAwait(false)
        )
        {
            yield return document.Value;
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static bool IsFinished(SagaState state) =>
        state
            is SagaState.Completed
                or SagaState.Failed
                or SagaState.Compensated
                or SagaState.CompensationFailed
                or SagaState.Cancelled;

    // The state is the index key, so sagas can be listed by state, oldest first
    private static DocumentWriteOptions WriteOptions(Saga saga) =>
        new() { IndexKey = saga.State.ToString(), SortKey = saga.CreatedAt };

    private static List<SagaStep> UpdateStep(
        Saga saga,
        string stepId,
        Func<SagaStep, SagaStep> update
    ) => saga.Steps.Select(step => step.Id == stepId ? update(step) : step).ToList();

    private static Saga Fail(Saga saga, string? errorMessage)
    {
        var needsCompensation = saga
            .Steps.Take(saga.CurrentStepIndex + 1)
            .Any(s => s.State == SagaStepState.Completed && s.RequiresCompensation);

        return saga with
        {
            State = needsCompensation ? SagaState.Compensating : SagaState.Failed,
            FailureReason = errorMessage ?? saga.FailureReason,
        };
    }

    private ValueTask<Saga?> UpdateAsync(
        string sagaId,
        Func<Saga, Saga> update,
        CancellationToken cancellationToken
    ) =>
        _documents.UpdateAsync(
            _sagas,
            sagaId,
            SagaTypeInfo,
            current => update(current.Value),
            WriteOptions,
            cancellationToken
        );

    private async ValueTask IndexTaskAsync(
        string sagaId,
        string? taskId,
        CancellationToken cancellationToken
    )
    {
        if (taskId is null)
        {
            return;
        }

        await _documents
            .UpsertAsync(
                _tasks,
                taskId,
                ReadOnlyMemory<byte>.Empty,
                new DocumentWriteOptions { IndexKey = sagaId },
                cancellationToken
            )
            .ConfigureAwait(false);
    }
}
