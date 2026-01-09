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
        ITaskSignalDispatcher? signalDispatcher = null
    )
    {
        _sagaStore = sagaStore;
        _broker = broker;
        _serializer = serializer;
        _backend = backend;
        _options = options.Value;
        _logger = logger;
        _signalDispatcher = signalDispatcher ?? NullTaskSignalDispatcher.Instance;
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
            StartedAt = DateTimeOffset.UtcNow,
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
                        Timestamp = DateTimeOffset.UtcNow,
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
                            Timestamp = DateTimeOffset.UtcNow,
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

        if (saga.State == SagaState.Compensating || saga.State == SagaState.Compensated)
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
                        Timestamp = DateTimeOffset.UtcNow,
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
                        Timestamp = DateTimeOffset.UtcNow,
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
                        Timestamp = DateTimeOffset.UtcNow,
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

    private async Task ExecuteNextStepAsync(Saga saga, CancellationToken cancellationToken)
    {
        var step = saga.CurrentStep;
        if (step is null || step.State != SagaStepState.Pending)
        {
            return;
        }

        _logger.LogDebug(
            "Executing step {StepOrder} '{StepName}' for saga {SagaId}",
            step.Order,
            step.Name,
            saga.Id
        );

        // Update step state to Executing
        await _sagaStore
            .UpdateStepStateAsync(
                saga.Id,
                step.Id,
                SagaStepState.Executing,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // Send the execution task
        var taskId = await SendSignatureAsync(
                step.ExecuteTask,
                saga.CorrelationId,
                cancellationToken
            )
            .ConfigureAwait(false);

        // Update step with task ID
        await _sagaStore
            .UpdateStepStateAsync(
                saga.Id,
                step.Id,
                SagaStepState.Executing,
                taskId: taskId,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Sent task {TaskId} for saga {SagaId} step {StepId}",
            taskId,
            saga.Id,
            step.Id
        );
    }

    internal async Task ExecuteNextCompensationAsync(Saga saga, CancellationToken cancellationToken)
    {
        // Find the next step to compensate (completed steps with compensation, in reverse order)
        var stepToCompensate = saga
            .Steps.Where(s => s.State == SagaStepState.Completed && s.RequiresCompensation)
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
                            Timestamp = DateTimeOffset.UtcNow,
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

        _logger.LogDebug(
            "Compensating step {StepOrder} '{StepName}' for saga {SagaId}",
            stepToCompensate.Order,
            stepToCompensate.Name,
            saga.Id
        );

        // Update step state to Compensating
        await _sagaStore
            .UpdateStepStateAsync(
                saga.Id,
                stepToCompensate.Id,
                SagaStepState.Compensating,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // Send the compensation task
        var compensateTask = stepToCompensate.CompensateTask!;
        var taskId = await SendSignatureAsync(compensateTask, saga.CorrelationId, cancellationToken)
            .ConfigureAwait(false);

        // Update step with compensation task ID (keep state as Compensating)
        await _sagaStore
            .UpdateStepStateAsync(
                saga.Id,
                stepToCompensate.Id,
                SagaStepState.Compensating,
                compensateTaskId: taskId,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Sent compensation task {TaskId} for saga {SagaId} step {StepId}",
            taskId,
            saga.Id,
            stepToCompensate.Id
        );
    }

    private async ValueTask<string> SendSignatureAsync(
        Signature signature,
        string? correlationId,
        CancellationToken cancellationToken
    )
    {
        var taskId = Guid.NewGuid().ToString("N");

        var eta = signature.Eta;
        if (signature.Countdown.HasValue)
        {
            eta = DateTimeOffset.UtcNow.Add(signature.Countdown.Value);
        }

        var message = new TaskMessage
        {
            Id = taskId,
            Task = signature.TaskName,
            Args = signature.Args ?? [],
            ContentType = _serializer.ContentType,
            Timestamp = DateTimeOffset.UtcNow,
            Eta = eta,
            Expires = signature.Expires,
            MaxRetries = signature.MaxRetries,
            Priority = signature.Priority,
            Queue = signature.Queue,
            CorrelationId = correlationId,
            Headers = signature.Headers,
        };

        await _broker.PublishAsync(message, cancellationToken).ConfigureAwait(false);
        await _backend
            .UpdateStateAsync(taskId, TaskState.Pending, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        return taskId;
    }
}
