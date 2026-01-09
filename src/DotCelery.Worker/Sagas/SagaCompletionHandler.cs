using DotCelery.Core.Sagas;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Sagas;

/// <summary>
/// Signal handler that updates saga state when tasks complete.
/// Follows the pattern established by BatchCompletionHandler.
/// </summary>
public sealed class SagaCompletionHandler
    : ITaskSignalHandler<TaskSuccessSignal>,
        ITaskSignalHandler<TaskFailureSignal>,
        ITaskSignalHandler<TaskRevokedSignal>,
        ITaskSignalHandler<TaskRejectedSignal>
{
    private readonly ISagaStore _sagaStore;
    private readonly ISagaOrchestrator _orchestrator;
    private readonly ITaskSignalDispatcher _signalDispatcher;
    private readonly SagaOptions _options;
    private readonly ILogger<SagaCompletionHandler> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="SagaCompletionHandler"/> class.
    /// </summary>
    public SagaCompletionHandler(
        ISagaStore sagaStore,
        ISagaOrchestrator orchestrator,
        IOptions<SagaOptions> options,
        ILogger<SagaCompletionHandler> logger,
        ITaskSignalDispatcher? signalDispatcher = null
    )
    {
        _sagaStore = sagaStore;
        _orchestrator = orchestrator;
        _options = options.Value;
        _logger = logger;
        _signalDispatcher = signalDispatcher ?? NullTaskSignalDispatcher.Instance;
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskSuccessSignal signal,
        CancellationToken cancellationToken
    )
    {
        var sagaId = await _sagaStore
            .GetSagaIdForTaskAsync(signal.TaskId, cancellationToken)
            .ConfigureAwait(false);

        if (sagaId is null)
        {
            return; // Not a saga task
        }

        var saga = await _sagaStore.GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null)
        {
            return;
        }

        // Find which step this task belongs to
        var step = saga.Steps.FirstOrDefault(s =>
            s.ExecuteTaskId == signal.TaskId || s.CompensateTaskId == signal.TaskId
        );

        if (step is null)
        {
            _logger.LogWarning(
                "Task {TaskId} belongs to saga {SagaId} but no matching step found",
                signal.TaskId,
                sagaId
            );
            return;
        }

        if (step.ExecuteTaskId == signal.TaskId)
        {
            // Execution step completed successfully
            await HandleStepSuccessAsync(saga, step, signal, cancellationToken)
                .ConfigureAwait(false);
        }
        else if (step.CompensateTaskId == signal.TaskId)
        {
            // Compensation step completed successfully
            await HandleCompensationSuccessAsync(saga, step, signal, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskFailureSignal signal,
        CancellationToken cancellationToken
    )
    {
        await HandleTaskFailureAsync(
                signal.TaskId,
                signal.Exception.Message,
                signal.Duration,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskRevokedSignal signal,
        CancellationToken cancellationToken
    )
    {
        await HandleTaskFailureAsync(
                signal.TaskId,
                "Task was revoked",
                TimeSpan.Zero,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskRejectedSignal signal,
        CancellationToken cancellationToken
    )
    {
        await HandleTaskFailureAsync(signal.TaskId, signal.Reason, TimeSpan.Zero, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task HandleStepSuccessAsync(
        Saga saga,
        SagaStep step,
        TaskSuccessSignal signal,
        CancellationToken cancellationToken
    )
    {
        _logger.LogDebug(
            "Step {StepId} completed successfully for saga {SagaId}",
            step.Id,
            saga.Id
        );

        // Update step state to Completed
        await _sagaStore
            .UpdateStepStateAsync(
                saga.Id,
                step.Id,
                SagaStepState.Completed,
                result: signal.Result,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // Dispatch step completed signal
        if (_options.DispatchSignals)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new SagaStepCompletedSignal
                    {
                        TaskId = signal.TaskId,
                        TaskName = step.Name,
                        Timestamp = DateTimeOffset.UtcNow,
                        SagaId = saga.Id,
                        StepId = step.Id,
                        StepOrder = step.Order,
                        State = SagaStepState.Completed,
                        Result = signal.Result,
                        Duration = signal.Duration,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        // Continue to next step if saga is still executing
        if (saga.State == SagaState.Executing)
        {
            await _orchestrator.ContinueAsync(saga.Id, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleCompensationSuccessAsync(
        Saga saga,
        SagaStep step,
        TaskSuccessSignal signal,
        CancellationToken cancellationToken
    )
    {
        _logger.LogDebug(
            "Compensation for step {StepId} completed successfully for saga {SagaId}",
            step.Id,
            saga.Id
        );

        // Mark step as compensated
        var updatedSaga = await _sagaStore
            .MarkStepCompensatedAsync(
                saga.Id,
                step.Id,
                success: true,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // Dispatch step compensated signal
        if (_options.DispatchSignals)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new SagaStepCompensatedSignal
                    {
                        TaskId = signal.TaskId,
                        TaskName = step.Name,
                        Timestamp = DateTimeOffset.UtcNow,
                        SagaId = saga.Id,
                        StepId = step.Id,
                        StepOrder = step.Order,
                        Success = true,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        // Continue compensation if saga is still compensating
        if (updatedSaga?.State == SagaState.Compensating)
        {
            // Need to get fresh saga state and continue compensation
            var freshSaga = await _sagaStore
                .GetAsync(saga.Id, cancellationToken)
                .ConfigureAwait(false);
            if (
                freshSaga?.State == SagaState.Compensating
                && _orchestrator is SagaOrchestrator orchestrator
            )
            {
                await orchestrator
                    .ExecuteNextCompensationAsync(freshSaga, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private async Task HandleTaskFailureAsync(
        string taskId,
        string errorMessage,
        TimeSpan duration,
        CancellationToken cancellationToken
    )
    {
        var sagaId = await _sagaStore
            .GetSagaIdForTaskAsync(taskId, cancellationToken)
            .ConfigureAwait(false);

        if (sagaId is null)
        {
            return; // Not a saga task
        }

        var saga = await _sagaStore.GetAsync(sagaId, cancellationToken).ConfigureAwait(false);
        if (saga is null)
        {
            return;
        }

        // Find which step this task belongs to
        var step = saga.Steps.FirstOrDefault(s =>
            s.ExecuteTaskId == taskId || s.CompensateTaskId == taskId
        );

        if (step is null)
        {
            return;
        }

        if (step.ExecuteTaskId == taskId)
        {
            // Execution step failed
            await HandleStepFailureAsync(
                    saga,
                    step,
                    taskId,
                    errorMessage,
                    duration,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        else if (step.CompensateTaskId == taskId)
        {
            // Compensation step failed
            await HandleCompensationFailureAsync(
                    saga,
                    step,
                    taskId,
                    errorMessage,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
    }

    private async Task HandleStepFailureAsync(
        Saga saga,
        SagaStep step,
        string taskId,
        string errorMessage,
        TimeSpan duration,
        CancellationToken cancellationToken
    )
    {
        _logger.LogWarning(
            "Step {StepId} failed for saga {SagaId}: {Error}",
            step.Id,
            saga.Id,
            errorMessage
        );

        // Update step state to Failed
        await _sagaStore
            .UpdateStepStateAsync(
                saga.Id,
                step.Id,
                SagaStepState.Failed,
                errorMessage: errorMessage,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // Dispatch step completed signal (with failed state)
        if (_options.DispatchSignals)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new SagaStepCompletedSignal
                    {
                        TaskId = taskId,
                        TaskName = step.Name,
                        Timestamp = DateTimeOffset.UtcNow,
                        SagaId = saga.Id,
                        StepId = step.Id,
                        StepOrder = step.Order,
                        State = SagaStepState.Failed,
                        Error = errorMessage,
                        Duration = duration,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        // Trigger compensation if auto-compensate is enabled and there are compensable steps
        if (_options.AutoCompensateOnFailure)
        {
            await _orchestrator
                .CompensateAsync(saga.Id, errorMessage, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    private async Task HandleCompensationFailureAsync(
        Saga saga,
        SagaStep step,
        string taskId,
        string errorMessage,
        CancellationToken cancellationToken
    )
    {
        _logger.LogError(
            "Compensation for step {StepId} failed for saga {SagaId}: {Error}",
            step.Id,
            saga.Id,
            errorMessage
        );

        // Mark step compensation as failed
        var updatedSaga = await _sagaStore
            .MarkStepCompensatedAsync(
                saga.Id,
                step.Id,
                success: false,
                errorMessage: errorMessage,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);

        // Dispatch step compensated signal (with failure)
        if (_options.DispatchSignals)
        {
            await _signalDispatcher
                .DispatchAsync(
                    new SagaStepCompensatedSignal
                    {
                        TaskId = taskId,
                        TaskName = step.Name,
                        Timestamp = DateTimeOffset.UtcNow,
                        SagaId = saga.Id,
                        StepId = step.Id,
                        StepOrder = step.Order,
                        Success = false,
                        Error = errorMessage,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        // Continue with remaining compensations even if one fails
        if (
            updatedSaga?.State == SagaState.Compensating
            && _orchestrator is SagaOrchestrator orchestrator
        )
        {
            var freshSaga = await _sagaStore
                .GetAsync(saga.Id, cancellationToken)
                .ConfigureAwait(false);
            if (freshSaga?.State == SagaState.Compensating)
            {
                await orchestrator
                    .ExecuteNextCompensationAsync(freshSaga, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }
}
