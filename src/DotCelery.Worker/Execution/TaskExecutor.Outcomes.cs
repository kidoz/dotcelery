using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Filters;
using DotCelery.Core.Instrumentation;
using DotCelery.Core.Models;
using DotCelery.Core.Signals;
using DotCelery.Worker.Registry;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

public sealed partial class TaskExecutor
{
    private async ValueTask<TaskResult> PublishSuccessSignalsAsync(
        TaskMessage message,
        TaskResult outcome,
        object? result,
        CancellationToken cancellationToken
    )
    {
        await _signalDispatcher
            .DispatchAsync(
                new TaskSuccessSignal
                {
                    TaskId = message.Id,
                    TaskName = message.Task,
                    Timestamp = _timeProvider.GetUtcNow(),
                    Result = result,
                    Duration = outcome.Duration,
                    Worker = outcome.Worker,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        await _signalDispatcher
            .DispatchAsync(
                new TaskPostRunSignal
                {
                    TaskId = message.Id,
                    TaskName = message.Task,
                    Timestamp = _timeProvider.GetUtcNow(),
                    State = TaskState.Success,
                    Duration = outcome.Duration,
                    Result = result,
                    Worker = outcome.Worker,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        var published = outcome with { SignalsPublished = true, IsDuplicate = false };
        await RecordOutcomeAsync(published, cancellationToken).ConfigureAwait(false);
        return published;
    }

    // Checkpoint only after publication. A crash before this write can replay the same
    // deterministic continuation ID; a completed duplicate never rewrites the outcome.
    internal ValueTask CompleteFollowUpsAsync(
        TaskResult result,
        CancellationToken cancellationToken
    ) =>
        RecordOutcomeAsync(
            result with
            {
                FollowUpsPublished = true,
                IsDuplicate = false,
            },
            cancellationToken
        );

    // A message the worker will not run is recorded as rejected, so a client sees a final state
    // and the dead letter queue keeps the reason
    private async Task<TaskResult> RecordRejectedAsync(
        TaskMessage message,
        Exception exception,
        DateTimeOffset startTime,
        string? workerName,
        CancellationToken cancellationToken
    )
    {
        var taskResult = new TaskResult
        {
            TaskId = message.Id,
            State = TaskState.Rejected,
            Exception = ResultExceptionCapture.Capture(exception, _options, _logger),
            CompletedAt = _timeProvider.GetUtcNow(),
            Duration = _timeProvider.GetUtcNow() - startTime,
            Retries = message.Retries,
            Worker = workerName,
        };

        await RecordOutcomeAsync(taskResult, cancellationToken).ConfigureAwait(false);

        await _signalDispatcher
            .DispatchAsync(
                new TaskRejectedSignal
                {
                    TaskId = message.Id,
                    TaskName = message.Task,
                    Timestamp = _timeProvider.GetUtcNow(),
                    Reason = exception.Message,
                    Worker = workerName,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        await _signalDispatcher
            .DispatchAsync(
                new TaskPostRunSignal
                {
                    TaskId = message.Id,
                    TaskName = message.Task,
                    Timestamp = _timeProvider.GetUtcNow(),
                    State = TaskState.Rejected,
                    Duration = taskResult.Duration,
                    Worker = workerName,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        return taskResult;
    }

    // The recorder stores the outcome and marks a successful message as processed in one
    // storage transaction when the result backend and the inbox store can share one.
    private ValueTask RecordOutcomeAsync(TaskResult result, CancellationToken cancellationToken) =>
        _outcomes is null
            ? _resultBackend.StoreResultAsync(result, cancellationToken: cancellationToken)
            : _outcomes.RecordAsync(result, cancellationToken);

    private async Task<TaskResult?> CheckRateLimitAsync(
        TaskMessage message,
        TaskRegistration registration,
        CancellationToken cancellationToken
    )
    {
        var policy = registration.RateLimitPolicy!;
        var resourceKey = policy.ResourceKey ?? message.Task;

        var lease = await _rateLimiter!
            .TryAcquireAsync(resourceKey, policy, cancellationToken)
            .ConfigureAwait(false);

        if (lease.IsAcquired)
        {
            _logger.LogDebug(
                "Rate limit acquired for task {TaskId}: {Remaining} remaining",
                message.Id,
                lease.Remaining
            );
            return null;
        }

        _logger.LogInformation(
            "Task {TaskId} rate limited, retry after {RetryAfter}",
            message.Id,
            lease.RetryAfter
        );

        // Return a rate-limited result (don't persist - task will be requeued and executed later)
        // The state will be persisted when the task actually executes
        // Set DoNotIncrementRetries because the task never executed - rate limit retries shouldn't count
        return new TaskResult
        {
            TaskId = message.Id,
            State = TaskState.Retry,
            RetryAfter = _options.RateLimitRequeueDelay ?? lease.RetryAfter,
            CompletedAt = _timeProvider.GetUtcNow(),
            Duration = TimeSpan.Zero,
            Retries = message.Retries,
            Worker = null,
            DoNotIncrementRetries = true,
        };
    }

    private async Task<TaskResult> CreateRevokedResultAsync(
        TaskMessage message,
        string? workerName,
        DateTimeOffset startTime,
        bool terminated,
        CancellationToken cancellationToken
    )
    {
        var taskResult = new TaskResult
        {
            TaskId = message.Id,
            State = TaskState.Revoked,
            CompletedAt = _timeProvider.GetUtcNow(),
            Duration = _timeProvider.GetUtcNow() - startTime,
            Retries = message.Retries,
            Worker = workerName,
        };

        await RecordOutcomeAsync(taskResult, cancellationToken).ConfigureAwait(false);

        // Dispatch revoked signals
        await _signalDispatcher
            .DispatchAsync(
                new TaskRevokedSignal
                {
                    TaskId = message.Id,
                    TaskName = message.Task,
                    Timestamp = _timeProvider.GetUtcNow(),
                    Terminated = terminated,
                    Worker = workerName,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        await _signalDispatcher
            .DispatchAsync(
                new TaskPostRunSignal
                {
                    TaskId = message.Id,
                    TaskName = message.Task,
                    Timestamp = _timeProvider.GetUtcNow(),
                    State = TaskState.Revoked,
                    Duration = taskResult.Duration,
                    Worker = workerName,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        return taskResult;
    }
}
