using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Threading.Channels;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.DeadLetter;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Instrumentation;
using DotCelery.Core.Models;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker;

public sealed partial class CeleryWorkerService
{
    /// <summary>
    /// Moves messages from the broker to the workers until intake stops or the broker ends
    /// the stream.
    /// </summary>
    /// <returns>A broker read that was still in progress when intake stopped, if any.</returns>
    private async Task<Task<bool>?> PumpMessagesAsync(
        IAsyncEnumerator<BrokerMessage> consumer,
        CancellationToken intakeToken
    )
    {
        while (!intakeToken.IsCancellationRequested)
        {
            // Stopping intake must not cancel the broker read itself: that would close the
            // consumer while tasks are still running.
            var read = consumer.MoveNextAsync().AsTask();

            try
            {
                if (!await read.WaitAsync(intakeToken).ConfigureAwait(false))
                {
                    return null;
                }
            }
            catch (OperationCanceledException) when (intakeToken.IsCancellationRequested)
            {
                return read;
            }

            await DispatchAsync(consumer.Current, intakeToken).ConfigureAwait(false);
        }

        return null;
    }

    private async Task DispatchAsync(BrokerMessage message, CancellationToken intakeToken)
    {
        try
        {
            // Wait if kill switch is tripped
            if (_killSwitch is not null)
            {
                await _killSwitch.WaitUntilReadyAsync(intakeToken).ConfigureAwait(false);
            }

            // Validate before the message reaches a worker, so a refused message is never
            // deserialized, recorded as started, or executed
            if (_securityValidator is not null || _executor.RequiresTenantAuthentication)
            {
                if (ValidateMessage(message) is { } rejection)
                {
                    _logger.LogWarning(
                        "Rejecting message {TaskId} for task {TaskName}: {Error}",
                        message.Message.Id,
                        message.Message.Task,
                        rejection.ErrorMessage
                    );

                    if (_executor.RequiresTenantAuthentication)
                    {
                        await _executor
                            .QuarantineDeliveryAsync(message, intakeToken)
                            .ConfigureAwait(false);
                        await _settler.AckAsync(message).ConfigureAwait(false);
                        return;
                    }

                    if (
                        !await _settler
                            .DeadLetterAsync(message, DeadLetterReason.Rejected)
                            .ConfigureAwait(false)
                    )
                    {
                        return;
                    }
                    await _settler.AckAsync(message).ConfigureAwait(false);
                    return;
                }
            }

            using var tenant = _executor.EnterTenantScope(message.Message);
            var now = _timeProvider.GetUtcNow();

            // Check if task has expired
            if (message.Message.Expires.HasValue && message.Message.Expires.Value < now)
            {
                _logger.LogWarning("Task {TaskId} has expired, skipping", message.Message.Id);
                if (
                    !await _settler
                        .DeadLetterAsync(message, DeadLetterReason.Expired)
                        .ConfigureAwait(false)
                )
                {
                    return;
                }
                await _settler.AckAsync(message).ConfigureAwait(false);
                return;
            }

            // A message whose ETA is still ahead goes to the delay store when one is configured.
            // Without one it is handed to a worker, which waits on it (WorkerOptions.MaxEtaWait)
            // in a task slot instead of holding this intake loop, so other queues keep flowing
            if (
                message.Message.Eta.HasValue
                && message.Message.Eta.Value > now
                && _options.UseDelayQueue
                && _delayedMessageStore is not null
            )
            {
                // Add to delay store for efficient handling
                await _delayedMessageStore
                    .AddAsync(message.Message, message.Message.Eta.Value, intakeToken)
                    .ConfigureAwait(false);

                await _settler.AckAsync(message).ConfigureAwait(false);

                _logger.LogDebug(
                    "Task {TaskId} scheduled for {Eta} via delay store",
                    message.Message.Id,
                    message.Message.Eta.Value
                );

                return;
            }

            await _workChannel.Writer.WriteAsync(message, intakeToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (intakeToken.IsCancellationRequested)
        {
            // Intake stopped before the message reached a worker
            await _settler.ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to dispatch message {TaskId}, returning it to the broker",
                message.Message.Id
            );
            await _settler.ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
    }

    private async Task ProcessMessagesAsync(
        int workerId,
        CancellationToken intakeToken,
        CancellationToken executionToken
    )
    {
        _logger.LogDebug("Worker thread {WorkerId} started", workerId);

        // Read until the dispatcher completes the channel so every dispatched message is settled
        await foreach (var message in _workChannel.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            // Register before checking for shutdown: a graceful shutdown that has already
            // started either waits for this task or sees the message returned to the broker.
            using var registration = _shutdownHandler?.RegisterTask(message.Message.Id, message);

            if (intakeToken.IsCancellationRequested)
            {
                // Prefetched but not started, so another worker can take it
                await _settler.ReturnToBrokerAsync(message).ConfigureAwait(false);
                continue;
            }

            await ProcessMessageAsync(message, executionToken).ConfigureAwait(false);
        }

        _logger.LogDebug("Worker thread {WorkerId} stopped", workerId);
    }

    private async Task ProcessMessageAsync(BrokerMessage message, CancellationToken executionToken)
    {
        if (message.ClaimLost.IsCancellationRequested)
            return;
        using var linked = message.ClaimLost.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(executionToken, message.ClaimLost)
            : null;
        using var tenant = _executor.EnterTenantScope(message.Message);
        var taskName = message.Message.Task;
        DotCeleryMetrics.IncrementTasksInProgress(taskName);
        _activity?.IncrementActiveTasks();

        try
        {
            await ProcessMessageCoreAsync(message, linked?.Token ?? executionToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (message.ClaimLost.IsCancellationRequested) { }
        finally
        {
            DotCeleryMetrics.DecrementTasksInProgress(taskName);
            _activity?.DecrementActiveTasks();
        }
    }

    private async Task ProcessMessageCoreAsync(
        BrokerMessage message,
        CancellationToken executionToken
    )
    {
        // A message that arrived before its ETA waits here, in one task slot, instead of in the
        // intake loop; when the wait does not cover the remaining time it goes back to the broker
        if (!await _etaWaiter.WaitAsync(message, executionToken).ConfigureAwait(false))
        {
            return;
        }

        DotCeleryMetrics.RecordTaskReceived(message.Message.Task, message.Message.Queue);
        DotCeleryMetrics.RecordQueueTime(
            message.Message.Task,
            _timeProvider.GetUtcNow() - message.Message.Timestamp
        );

        // While a queue's circuit is open (or the global one, after failures that affected
        // every queue), its messages wait in the broker instead of running
        if (_circuits.OpenFor(message) is { } openCircuit)
        {
            _logger.LogWarning(
                "Circuit '{Circuit}' is open; returning task {TaskId} of queue {Queue} to the broker",
                openCircuit.Name,
                message.Message.Id,
                message.Queue
            );
            await _settler
                .ReturnAfterFailureAsync(
                    message,
                    _options.InfrastructureFailureRequeueDelay,
                    executionToken
                )
                .ConfigureAwait(false);
            return;
        }

        TaskResult result;

        try
        {
            result = await _executor
                .ExecuteAsync(message, _workerName, executionToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (message.ClaimLost.IsCancellationRequested)
        {
            return;
        }
        catch (OperationCanceledException) when (executionToken.IsCancellationRequested)
        {
            await HandleInterruptedAsync(message).ConfigureAwait(false);
            return;
        }
        catch (Exception) when (message.ClaimLost.IsCancellationRequested)
        {
            return;
        }
        catch (UnknownTaskException ex)
        {
            // Redelivering a message that no worker can handle would loop forever, so it is kept
            // as a dead letter and settled instead
            _killSwitch?.RecordFailure(ex);
            _logger.LogError(
                ex,
                "Rejecting message {TaskId} for unregistered task {TaskName}",
                message.Message.Id,
                message.Message.Task
            );
            if (
                !await _settler
                    .DeadLetterAsync(message, DeadLetterReason.UnknownTask)
                    .ConfigureAwait(false)
            )
            {
                return;
            }
            await _settler.RejectAsync(message).ConfigureAwait(false);
            return;
        }
        catch (JsonException ex)
        {
            // A payload that cannot be read will never become readable, so returning it to the
            // broker would loop forever; it is kept as a dead letter and settled instead
            _killSwitch?.RecordFailure(ex);
            _logger.LogError(
                ex,
                "Rejecting message {TaskId} for task {TaskName}: its payload cannot be read",
                message.Message.Id,
                message.Message.Task
            );
            if (
                !await _settler
                    .DeadLetterAsync(message, DeadLetterReason.Rejected)
                    .ConfigureAwait(false)
            )
            {
                return;
            }
            await _settler.RejectAsync(message).ConfigureAwait(false);
            return;
        }
        catch (Exception ex)
        {
            // The outcome was not recorded (for example, the result backend is unavailable)
            _killSwitch?.RecordFailure(ex);
            _circuits.RecordFailure(message, ex);
            _logger.LogError(
                ex,
                "Failed to process message {TaskId}, returning it to the broker",
                message.Message.Id
            );
            await _settler
                .ReturnAfterFailureAsync(
                    message,
                    _options.InfrastructureFailureRequeueDelay,
                    executionToken
                )
                .ConfigureAwait(false);
            return;
        }

        if (message.ClaimLost.IsCancellationRequested)
            return;

        // Record success/failure for kill switch
        if (result.State == TaskState.Success)
        {
            _killSwitch?.RecordSuccess();
        }
        else if (result.State == TaskState.Failure || result.State == TaskState.Rejected)
        {
            _killSwitch?.RecordFailure();
        }

        if (result.State == TaskState.Retry)
        {
            DotCeleryMetrics.RecordTaskRetry(message.Message.Task, result.Retries);

            try
            {
                await _retries.ScheduleAsync(message, result).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // The retry was not scheduled, so keep the original message
                _logger.LogError(
                    ex,
                    "Failed to schedule retry for task {TaskId}, returning the message to the broker",
                    message.Message.Id
                );
                await _settler
                    .ReturnAfterFailureAsync(
                        message,
                        _options.InfrastructureFailureRequeueDelay,
                        executionToken
                    )
                    .ConfigureAwait(false);
                return;
            }

            await _settler.AckAsync(message).ConfigureAwait(false);
        }
        else if (result.State == TaskState.Requeued)
        {
            // Requeue the message for later processing (e.g., partition locked)
            // Apply requeue delay to prevent hot loops
            if (result.RequeueDelay.HasValue && result.RequeueDelay.Value > TimeSpan.Zero)
            {
                _logger.LogDebug(
                    "Task {TaskId} requeued with delay {RequeueDelay}",
                    message.Message.Id,
                    result.RequeueDelay.Value
                );
                await _settler
                    .DelayAsync(result.RequeueDelay.Value, executionToken)
                    .ConfigureAwait(false);
            }
            else
            {
                _logger.LogDebug("Task {TaskId} requeued for later processing", message.Message.Id);
            }

            await _settler.ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
        else
        {
            // Success, Failure, Rejected, and Revoked outcomes are already stored
            if (!result.IsDuplicate)
            {
                DotCeleryMetrics.RecordTaskCompleted(
                    message.Message.Task,
                    result.State == TaskState.Success,
                    result.Duration
                );
                _activity?.IncrementProcessedTasks();
            }

            if (result.State == TaskState.Success && !result.FollowUpsPublished)
            {
                if (
                    !await _followUps
                        .ContinueChainAsync(message, result, executionToken)
                        .ConfigureAwait(false)
                )
                {
                    return;
                }

                if (
                    !await _followUps
                        .ContinueWithLinkAsync(message, result, executionToken)
                        .ConfigureAwait(false)
                )
                {
                    return;
                }
                try
                {
                    await _executor
                        .CompleteFollowUpsAsync(result, executionToken)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to checkpoint follow-ups for task {TaskId}",
                        result.TaskId
                    );
                    await _settler
                        .ReturnAfterFailureAsync(
                            message,
                            _options.InfrastructureFailureRequeueDelay,
                            executionToken
                        )
                        .ConfigureAwait(false);
                    return;
                }
            }
            else if (result.State == TaskState.Failure)
            {
                if (
                    !await _followUps
                        .ContinueWithErrorLinkAsync(message, result, executionToken)
                        .ConfigureAwait(false)
                )
                {
                    return;
                }
            }
            else if (result.State == TaskState.Rejected)
            {
                // A rejected message is handled, not processed; the dead letter queue keeps a
                // record of it instead of dropping it silently
                if (
                    !await _settler
                        .DeadLetterAsync(message, DeadLetterReason.Rejected)
                        .ConfigureAwait(false)
                )
                {
                    return;
                }
            }

            await _settler.AckAsync(message).ConfigureAwait(false);

            if (result.State == TaskState.Success)
            {
                _circuits.RecordSuccess(message);
            }
        }
    }

    // A message is refused when the security validator rejects it or its raw body does not
    // match its signature
    private MessageValidationResult? ValidateMessage(BrokerMessage message)
    {
        if (_securityValidator is null)
            return _executor.ValidateTenantDelivery(message) is { } error
                ? MessageValidationResult.Failure(MessageValidationError.TaskNotAllowed, error)
                : null;
        var result = _securityValidator.Validate(message.Message, message.Signature);
        if (!result.IsValid)
        {
            return result;
        }

        if (!string.IsNullOrEmpty(message.Signature) && message.RawBody is not { Length: > 0 })
            return MessageValidationResult.Failure(
                MessageValidationError.InvalidSignature,
                "Signed deliveries require their original serialized body."
            );

        if (
            message.RawBody is { Length: > 0 } rawBody
            && !_securityValidator.IsSignatureValid(result, rawBody, message.Signature)
        )
        {
            return MessageValidationResult.Failure(
                MessageValidationError.InvalidSignature,
                "Message signature is invalid"
            );
        }

        if (_executor.ValidateTenantDelivery(message) is { } tenantFailure)
            return MessageValidationResult.Failure(
                MessageValidationError.TaskNotAllowed,
                tenantFailure
            );
        return null;
    }

    private async Task HandleInterruptedAsync(BrokerMessage message)
    {
        if (_options.NackOnForcedShutdown)
        {
            _logger.LogWarning(
                "Task {TaskId} did not finish before shutdown, returning it to the broker",
                message.Message.Id
            );
            await _settler.ReturnToBrokerAsync(message).ConfigureAwait(false);
        }
        else
        {
            _logger.LogWarning(
                "Task {TaskId} did not finish before shutdown, leaving its message unacknowledged",
                message.Message.Id
            );
        }
    }
}
