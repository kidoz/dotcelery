using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;
using DotCelery.Core.Sagas;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Sagas;

/// <summary>
/// Default implementation of <see cref="ISagaOrchestrator"/>.
/// Orchestrates saga execution with step coordination and compensation.
/// </summary>
public sealed class SagaOrchestrator : ISagaOrchestrator
{
    private readonly ISagaStore _sagaStore;
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly IResultBackend _backend;
    private readonly ITaskSignalDispatcher _signalDispatcher;
    private readonly SagaOptions _options;
    private readonly ILogger<SagaOrchestrator> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="SagaOrchestrator"/> class.
    /// </summary>
    public SagaOrchestrator(
        ISagaStore sagaStore,
        IMessageBroker broker,
        IMessageSerializer serializer,
        IResultBackend backend,
        IOptions<SagaOptions> options,
        ILogger<SagaOrchestrator> logger,
        ITaskSignalDispatcher? signalDispatcher = null,
        TimeProvider? timeProvider = null
    )
    {
        _sagaStore = sagaStore;
        _broker = broker;
        _serializer = serializer;
        _backend = backend;
        _options = options.Value;
        _logger = logger;
        _signalDispatcher = signalDispatcher ?? NullTaskSignalDispatcher.Instance;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask<Saga> StartAsync(
        Saga saga,
        CancellationToken cancellationToken = default
    )
    {
        // Create saga with generated ID if not provided
        var newSaga = saga with
        {
            Id = string.IsNullOrEmpty(saga.Id) ? Guid.NewGuid().ToString() : saga.Id,
            State = SagaState.Executing,
            StartedAt = _timeProvider.GetUtcNow(),
            CurrentStepIndex = 0,
        };

        await _sagaStore.CreateAsync(newSaga, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Starting saga {SagaId} '{SagaName}' with {StepCount} steps",
            newSaga.Id,
            newSaga.Name,
            newSaga.TotalSteps
        );

        // Dispatch state changed signal
        if (_options.DispatchSignals)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new SagaStateChangedSignal
                    {
                        TaskId = newSaga.Id,
                        TaskName = newSaga.Name,
                        Timestamp = _timeProvider.GetUtcNow(),
                        OldState = SagaState.Created,
                        NewState = SagaState.Executing,
                        CurrentStepIndex = 0,
                        Progress = 0,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        // Start first step
        await ExecuteNextStepAsync(newSaga, cancellationToken).ConfigureAwait(false);

        return newSaga;
    }

    /// <inheritdoc />
    public async ValueTask<Saga?> ContinueAsync(
        string sagaId,
        CancellationToken cancellationToken = default
    )
    {
        var saga = await _sagaStore.GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null || saga.IsFinished)
        {
            return saga;
        }

        // Advance to next step
        var updatedSaga = await _sagaStore
            .AdvanceStepAsync(sagaId, cancellationToken)
            .ConfigureAwait(false);
        if (updatedSaga is null)
        {
            return null;
        }

        if (updatedSaga.IsFinished)
        {
            _logger.LogInformation(
                "Saga {SagaId} completed with state {State}",
                sagaId,
                updatedSaga.State
            );

            if (_options.DispatchSignals)
            {
                await _signalDispatcher
                    .DispatchAsync(
                        new SagaStateChangedSignal
                        {
                            TaskId = sagaId,
                            TaskName = updatedSaga.Name,
                            Timestamp = _timeProvider.GetUtcNow(),
                            OldState = SagaState.Executing,
                            NewState = updatedSaga.State,
                            CurrentStepIndex = updatedSaga.CurrentStepIndex,
                            Progress = updatedSaga.Progress,
                        },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }

            return updatedSaga;
        }

        // Execute next step
        await ExecuteNextStepAsync(updatedSaga, cancellationToken).ConfigureAwait(false);

        return updatedSaga;
    }

    /// <inheritdoc />
    public async ValueTask CompensateAsync(
        string sagaId,
        string? reason = null,
        CancellationToken cancellationToken = default
    )
    {
        var saga = await _sagaStore.GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null)
        {
            _logger.LogWarning("Cannot compensate saga {SagaId}: not found", sagaId);
            return;
        }

        // A saga whose step failed is already marked Compensating while its compensation is
        // still to run, so the state alone does not mean compensation is underway: a step being
        // compensated, or a finished compensation, does.
        if (
            saga.State is SagaState.Compensated or SagaState.CompensationFailed
            || saga.Steps.Any(step => step.State == SagaStepState.Compensating)
        )
        {
            _logger.LogWarning("Saga {SagaId} is already compensating or compensated", sagaId);
            return;
        }

        var oldState = saga.State;

        // Update saga state to Compensating
        saga = await _sagaStore
            .UpdateStateAsync(sagaId, SagaState.Compensating, reason, cancellationToken)
            .ConfigureAwait(false);

        if (saga is null)
        {
            return;
        }

        _logger.LogInformation(
            "Starting compensation for saga {SagaId}: {Reason}",
            sagaId,
            reason ?? "manual trigger"
        );

        // Dispatch compensation started signal
        if (_options.DispatchSignals)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new SagaCompensationStartedSignal
                    {
                        TaskId = sagaId,
                        TaskName = saga.Name,
                        Timestamp = _timeProvider.GetUtcNow(),
                        Reason = reason,
                        StepsToCompensate = saga.StepsToCompensate.Count,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            await _signalDispatcher
                .DispatchAsync(
                    new SagaStateChangedSignal
                    {
                        TaskId = sagaId,
                        TaskName = saga.Name,
                        Timestamp = _timeProvider.GetUtcNow(),
                        OldState = oldState,
                        NewState = SagaState.Compensating,
                        Reason = reason,
                        CurrentStepIndex = saga.CurrentStepIndex,
                        Progress = saga.Progress,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        // Start compensation from the last completed step (reverse order)
        await ExecuteNextCompensationAsync(saga, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask<Saga?> GetAsync(string sagaId, CancellationToken cancellationToken = default)
    {
        return _sagaStore.GetAsync(sagaId, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask RetryAsync(string sagaId, CancellationToken cancellationToken = default)
    {
        var saga = await _sagaStore.GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null)
        {
            _logger.LogWarning("Cannot retry saga {SagaId}: not found", sagaId);
            return;
        }

        if (saga.State != SagaState.Failed && saga.State != SagaState.CompensationFailed)
        {
            _logger.LogWarning("Cannot retry saga {SagaId}: state is {State}", sagaId, saga.State);
            return;
        }

        _logger.LogInformation("Retrying saga {SagaId} from last failed step", sagaId);

        // Reset the current step to Pending so it can be re-executed
        var currentStep = saga.CurrentStep;
        if (
            currentStep is not null
            && currentStep.State is SagaStepState.Executing or SagaStepState.Failed
        )
        {
            await _sagaStore
                .UpdateStepStateAsync(
                    sagaId,
                    currentStep.Id,
                    SagaStepState.Pending,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
        }

        // Update state back to Executing
        saga = await _sagaStore
            .UpdateStateAsync(sagaId, SagaState.Executing, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        if (saga is not null)
        {
            // Re-execute the current step
            await ExecuteNextStepAsync(saga, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask CancelAsync(
        string sagaId,
        string? reason = null,
        CancellationToken cancellationToken = default
    )
    {
        var saga = await _sagaStore.GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null)
        {
            _logger.LogWarning("Cannot cancel saga {SagaId}: not found", sagaId);
            return;
        }

        if (saga.IsFinished)
        {
            _logger.LogWarning("Cannot cancel saga {SagaId}: already finished", sagaId);
            return;
        }

        var oldState = saga.State;

        // If there are completed steps that need compensation, start compensation
        if (saga.StepsToCompensate.Count > 0)
        {
            await CompensateAsync(sagaId, reason ?? "Cancelled", cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        // Otherwise, just mark as cancelled
        saga = await _sagaStore
            .UpdateStateAsync(sagaId, SagaState.Cancelled, reason, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("Saga {SagaId} cancelled: {Reason}", sagaId, reason);

        if (saga is not null && _options.DispatchSignals)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new SagaStateChangedSignal
                    {
                        TaskId = sagaId,
                        TaskName = saga.Name,
                        Timestamp = _timeProvider.GetUtcNow(),
                        OldState = oldState,
                        NewState = SagaState.Cancelled,
                        Reason = reason,
                        CurrentStepIndex = saga.CurrentStepIndex,
                        Progress = saga.Progress,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
    }

    internal async Task ExecuteNextStepAsync(Saga saga, CancellationToken cancellationToken)
    {
        var step = saga.CurrentStep;
        if (
            step is null
            || step.State is not (SagaStepState.Pending or SagaStepState.Executing)
            || step.ExecutionDispatched
        )
            return;
        var taskId =
            step.ExecuteTaskId
            ?? TaskMessageFactory.ContinuationId(
                saga.Id,
                step.Id,
                "execute",
                step.ExecutionAttempt
            );
        var claimed = await _sagaStore
            .TryClaimStepDispatchAsync(
                saga.Id,
                step.Id,
                taskId,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        if (claimed is null)
            return;
        await SendSignatureAsync(step.ExecuteTask, saga.CorrelationId, cancellationToken, taskId)
            .ConfigureAwait(false);
        await _sagaStore
            .CompleteStepDispatchAsync(
                saga.Id,
                step.Id,
                taskId,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }

    internal async Task ExecuteNextCompensationAsync(Saga saga, CancellationToken cancellationToken)
    {
        // Find the next step to compensate (completed steps with compensation, in reverse order)
        var inFlight = saga.Steps.FirstOrDefault(step => step.State == SagaStepState.Compensating);
        if (inFlight?.CompensationDispatched == true)
            return;
        var stepToCompensate =
            inFlight
            ?? saga.Steps.Where(s => s.State == SagaStepState.Completed && s.RequiresCompensation)
                .OrderByDescending(s => s.Order)
                .FirstOrDefault();

        if (stepToCompensate is null)
        {
            // No more steps to compensate
            var anyFailed = saga.Steps.Any(s => s.State == SagaStepState.CompensationFailed);
            var newState = anyFailed ? SagaState.CompensationFailed : SagaState.Compensated;

            await _sagaStore
                .UpdateStateAsync(saga.Id, newState, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Saga {SagaId} compensation finished with state {State}",
                saga.Id,
                newState
            );

            if (_options.DispatchSignals)
            {
                await _signalDispatcher
                    .DispatchAsync(
                        new SagaStateChangedSignal
                        {
                            TaskId = saga.Id,
                            TaskName = saga.Name,
                            Timestamp = _timeProvider.GetUtcNow(),
                            OldState = SagaState.Compensating,
                            NewState = newState,
                            CurrentStepIndex = saga.CurrentStepIndex,
                            Progress = saga.Progress,
                        },
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }

            return;
        }

        var taskId =
            stepToCompensate.CompensateTaskId
            ?? TaskMessageFactory.ContinuationId(
                saga.Id,
                stepToCompensate.Id,
                "compensate",
                stepToCompensate.ExecutionAttempt
            );
        var claimed = await _sagaStore
            .TryClaimStepDispatchAsync(
                saga.Id,
                stepToCompensate.Id,
                taskId,
                compensation: true,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        if (claimed is null)
            return;
        await SendSignatureAsync(
                stepToCompensate.CompensateTask!,
                saga.CorrelationId,
                cancellationToken,
                taskId
            )
            .ConfigureAwait(false);
        await _sagaStore
            .CompleteStepDispatchAsync(
                saga.Id,
                stepToCompensate.Id,
                taskId,
                compensation: true,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
    }

    private async ValueTask<string> SendSignatureAsync(
        Signature signature,
        string? correlationId,
        CancellationToken cancellationToken,
        string? taskId = null
    )
    {
        taskId ??= Guid.NewGuid().ToString("N");

        var eta = signature.Eta;
        if (signature.Countdown.HasValue)
        {
            eta = _timeProvider.GetUtcNow().Add(signature.Countdown.Value);
        }

        var message = TaskMessageFactory.ForTask(
            taskId,
            signature.TaskName,
            signature.Args ?? [],
            _serializer.ContentType,
            _timeProvider.GetUtcNow(),
            signature.Queue,
            eta: eta,
            expires: signature.Expires,
            maxRetries: signature.MaxRetries,
            priority: signature.Priority,
            correlationId: correlationId,
            headers: signature.Headers
        );

        await _backend
            .UpdateStateAsync(taskId, TaskState.Pending, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        await _broker.PublishAsync(message, cancellationToken).ConfigureAwait(false);

        return taskId;
    }
}
