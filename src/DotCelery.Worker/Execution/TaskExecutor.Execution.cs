using System.Diagnostics;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Filters;
using DotCelery.Core.Instrumentation;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Resilience;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Resilience;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Execution;

public sealed partial class TaskExecutor
{
    /// <summary>
    /// Executes a task from a broker message.
    /// </summary>
    /// <param name="brokerMessage">The broker message.</param>
    /// <param name="workerName">The worker name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task result.</returns>
    /// <remarks>
    /// Exceptions thrown by the task are returned as a <see cref="TaskState.Failure"/> result.
    /// Any exception thrown by this method means the task outcome was not recorded.
    /// </remarks>
    /// <exception cref="UnknownTaskException">The task is not registered.</exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was cancelled before the outcome was recorded.
    /// </exception>
    public async Task<TaskResult> ExecuteAsync(
        BrokerMessage brokerMessage,
        string? workerName,
        CancellationToken cancellationToken
    )
    {
        if (ValidateTenantDelivery(brokerMessage) is { } tenantFailure)
            throw new RejectException(tenantFailure);
        using var deliveryTenant = EnterTenantScope(brokerMessage.Message);
        var message = brokerMessage.Message;
        var startTime = _timeProvider.GetUtcNow();

        using var processActivity = StartProcessActivity(message);

        var registration = _registry.GetTask(message.Task);
        if (registration is null)
        {
            _logger.LogError("Unknown task: {TaskName}", message.Task);
            throw new UnknownTaskException(message.Task);
        }

        // A task's [Retry] attribute wins over the worker's default policy
        var retryPolicy = registration.RetryPolicy ?? _resilienceOptions?.DefaultRetryPolicy;

        // Check revocation before execution
        if (_options.EnableRevocation && _options.CheckRevocationBeforeExecution)
        {
            if (
                await _revocationManager
                    .IsRevokedAsync(message.Id, cancellationToken)
                    .ConfigureAwait(false)
            )
            {
                _logger.LogInformation("Task {TaskId} was revoked, skipping execution", message.Id);
                return await CreateRevokedResultAsync(
                        message,
                        workerName,
                        startTime,
                        terminated: false, // Task never started, just prevented from running
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
        }

        // Check rate limiting before execution
        if (
            _options.EnableRateLimiting
            && registration.RateLimitPolicy is not null
            && _rateLimiter is not null
        )
        {
            var rateLimitResult = await CheckRateLimitAsync(
                    message,
                    registration,
                    cancellationToken
                )
                .ConfigureAwait(false);

            if (rateLimitResult is not null)
            {
                return rateLimitResult;
            }
        }

        _logger.LogDebug("Executing task {TaskName} with ID {TaskId}", message.Task, message.Id);

        // Register task for revocation and get linked token
        using var taskCts = _revocationManager.RegisterTask(message.Id, cancellationToken);
        var taskToken = taskCts.Token;

        // Only exceptions from the task body and its completion filters are task failures.
        // Startup, short-circuiting and outcome persistence failures must be redelivered.
        var taskBodyRunning = false;

        try
        {
            await using var scope = _serviceProvider.CreateAsyncScope();

            // Resolve task instance
            var task = scope.ServiceProvider.GetRequiredService(registration.TaskType);

            // Deserialize input
            object? input = null;
            if (registration.InputType is not null)
            {
                try
                {
                    input = _serializer.Deserialize(message.Args, registration.InputType);
                }
                catch (Exception ex) when (ex is JsonException or NotSupportedException)
                {
                    // A payload that cannot be read will never become readable, so it is a
                    // rejected message rather than a task failure that is retried
                    _logger.LogError(ex, "Task {TaskId} payload cannot be read", message.Id);
                    return await RecordRejectedAsync(
                            message,
                            ex,
                            startTime,
                            workerName,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                }
            }

            // Create execution context
            var context = new TaskExecutionContext(
                message: message,
                serviceProvider: scope.ServiceProvider,
                resultBackend: _resultBackend,
                signalDispatcher: _signalDispatcher,
                workerName: workerName,
                timeProvider: _timeProvider,
                maxRetriesLimit: _resilienceOptions?.MaxRetriesLimit
            );

            // Get filters for this task
            var filters = _filterPipeline.GetFilters(registration, scope);

            // Execute task with filters
            var (result, duration, requeueRequested, requeueDelay) = await _invocation
                .ExecuteWithFiltersAsync(
                    task,
                    input,
                    context,
                    registration,
                    filters,
                    brokerMessage,
                    async () =>
                    {
                        // Update state to Started
                        await _resultBackend
                            .UpdateStateAsync(
                                message.Id,
                                TaskState.Started,
                                cancellationToken: cancellationToken
                            )
                            .ConfigureAwait(false);

                        // Dispatch TaskPreRun signal
                        await _signalDispatcher
                            .DispatchAsync(
                                new TaskPreRunSignal
                                {
                                    TaskId = message.Id,
                                    TaskName = message.Task,
                                    Timestamp = _timeProvider.GetUtcNow(),
                                    Input = input,
                                    RetryCount = message.Retries,
                                    Worker = workerName,
                                },
                                cancellationToken
                            )
                            .ConfigureAwait(false);

                        taskBodyRunning = true;
                    },
                    taskToken
                )
                .ConfigureAwait(false);

            taskBodyRunning = false;

            // Check if requeue was requested by a filter
            if (requeueRequested)
            {
                _logger.LogDebug("Task {TaskId} filter requested requeue", message.Id);

                var requeueResult = new TaskResult
                {
                    TaskId = message.Id,
                    State = TaskState.Requeued,
                    CompletedAt = _timeProvider.GetUtcNow(),
                    Duration = duration,
                    Retries = message.Retries,
                    Worker = workerName,
                    RequeueDelay = requeueDelay,
                };

                // Persist the requeued state so dashboards/clients can see it
                await _resultBackend
                    .UpdateStateAsync(
                        message.Id,
                        TaskState.Requeued,
                        cancellationToken: cancellationToken
                    )
                    .ConfigureAwait(false);

                // Dispatch requeue signal
                await _signalDispatcher
                    .DispatchAsync(
                        new TaskRequeuedSignal
                        {
                            TaskId = message.Id,
                            TaskName = message.Task,
                            Timestamp = _timeProvider.GetUtcNow(),
                            Reason = "Filter requested requeue",
                            Worker = workerName,
                        },
                        cancellationToken
                    )
                    .ConfigureAwait(false);

                return requeueResult;
            }

            // Check if a filter provided a complete TaskResult (e.g., security or deduplication)
            if (result is TaskResult filterProvidedResult)
            {
                if (filterProvidedResult.IsDuplicate)
                {
                    var original = await _resultBackend
                        .GetResultAsync(message.Id, cancellationToken)
                        .ConfigureAwait(false);
                    if (original is null && _inboxOutcomes is not null)
                        original = await _inboxOutcomes
                            .GetOutcomeAsync(message.Id, cancellationToken)
                            .ConfigureAwait(false);
                    if (original is null)
                    {
                        if (message.Chain is not { Count: > 0 } && message.Link is null)
                            return filterProvidedResult with { FollowUpsPublished = true };
                        // Legacy markers may predate retained outcomes. Missing data is a
                        // permanent recovery failure, not a transient infrastructure outage.
                        return await RecordRejectedAsync(
                                message,
                                new InvalidOperationException(
                                    "The completed task's continuation outcome is unavailable."
                                ),
                                startTime,
                                workerName,
                                cancellationToken
                            )
                            .ConfigureAwait(false);
                    }
                    if (original.State == TaskState.Success && !original.SignalsPublished)
                    {
                        var output =
                            original.Result is not null && registration.OutputType is not null
                                ? _serializer.Deserialize(original.Result, registration.OutputType)
                                : null;
                        original = await PublishSuccessSignalsAsync(
                                message,
                                original,
                                output,
                                cancellationToken
                            )
                            .ConfigureAwait(false);
                    }
                    return original with { IsDuplicate = true };
                }

                // Store the result if it's a terminal state
                if (
                    filterProvidedResult.State
                    is TaskState.Success
                        or TaskState.Rejected
                        or TaskState.Failure
                )
                {
                    await RecordOutcomeAsync(filterProvidedResult, cancellationToken)
                        .ConfigureAwait(false);
                }

                _logger.LogInformation(
                    "Task {TaskId} completed with filter-provided result: {State}",
                    message.Id,
                    filterProvidedResult.State
                );

                if (filterProvidedResult.State == TaskState.Success)
                {
                    var output =
                        filterProvidedResult.Result is not null
                        && registration.OutputType is not null
                            ? _serializer.Deserialize(
                                filterProvidedResult.Result,
                                registration.OutputType
                            )
                            : null;
                    return await PublishSuccessSignalsAsync(
                            message,
                            filterProvidedResult,
                            output,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                }
                return filterProvidedResult;
            }

            var taskResult = new TaskResult
            {
                TaskId = message.Id,
                State = TaskState.Success,
                FollowUpsPublished = message.Chain is not { Count: > 0 } && message.Link is null,
                Result = result is not null ? _serializer.Serialize(result) : null,
                ContentType = _serializer.ContentType,
                CompletedAt = _timeProvider.GetUtcNow(),
                Duration = duration,
                Retries = message.Retries,
                Worker = workerName,
            };

            await RecordOutcomeAsync(taskResult, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "Task {TaskId} completed successfully in {Duration}ms",
                message.Id,
                duration.TotalMilliseconds
            );

            taskResult = await PublishSuccessSignalsAsync(
                    message,
                    taskResult,
                    result,
                    cancellationToken
                )
                .ConfigureAwait(false);

            return taskResult;
        }
        catch (OperationCanceledException)
            when (taskBodyRunning
                && taskToken.IsCancellationRequested
                && !cancellationToken.IsCancellationRequested
            )
        {
            // Task was revoked and terminated during execution
            _logger.LogInformation("Task {TaskId} was cancelled due to revocation", message.Id);
            return await CreateRevokedResultAsync(
                    message,
                    workerName,
                    startTime,
                    terminated: true, // Task was running and got cancelled
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The worker is stopping. This is not a task failure: the worker returns the
            // message to the broker so the task runs again.
            _logger.LogInformation(
                "Task {TaskId} was interrupted because the worker is stopping",
                message.Id
            );
            throw;
        }
        catch (RetryException ex)
        {
            _logger.LogInformation(
                "Task {TaskId} requested retry: {Message}",
                message.Id,
                ex.Message
            );

            // An explicit countdown always wins; without one, the effective policy's
            // backoff applies, and without a policy the task is republished immediately
            var retryAfter = ex.Countdown ?? retryPolicy?.ComputeDelay(message.Retries);

            var taskResult = new TaskResult
            {
                TaskId = message.Id,
                State = TaskState.Retry,
                Exception = ResultExceptionCapture.Capture(
                    ex.InnerException ?? ex,
                    _options,
                    _logger
                ),
                CompletedAt = _timeProvider.GetUtcNow(),
                Duration = _timeProvider.GetUtcNow() - startTime,
                Retries = message.Retries,
                Worker = workerName,
                RetryAfter = retryAfter,
            };

            await RecordOutcomeAsync(taskResult, cancellationToken).ConfigureAwait(false);

            // Dispatch retry signals
            await _signalDispatcher
                .DispatchAsync(
                    new TaskRetrySignal
                    {
                        TaskId = message.Id,
                        TaskName = message.Task,
                        Timestamp = _timeProvider.GetUtcNow(),
                        Exception = ex.InnerException ?? ex,
                        RetryCount = message.Retries,
                        Countdown = ex.Countdown,
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
                        State = TaskState.Retry,
                        Duration = taskResult.Duration,
                        Worker = workerName,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            return taskResult;
        }
        catch (RejectException ex)
        {
            _logger.LogWarning("Task {TaskId} was rejected: {Message}", message.Id, ex.Message);

            return await RecordRejectedAsync(message, ex, startTime, workerName, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (taskBodyRunning)
        {
            // Automatic retry: a retryable failure with retries remaining becomes a retry
            // with the policy's backoff instead of a failure; exhaustion falls through
            if (
                _resilienceOptions is { AutoRetryOnFailure: true }
                && retryPolicy is not null
                && retryPolicy.ShouldRetry(ex)
                && message.Retries < retryPolicy.MaxRetries
            )
            {
                var retryAfter = retryPolicy.ComputeDelay(message.Retries);

                _logger.LogInformation(
                    ex,
                    "Task {TaskId} failed and will be retried after {RetryAfter}",
                    message.Id,
                    retryAfter
                );

                var retryResult = new TaskResult
                {
                    TaskId = message.Id,
                    State = TaskState.Retry,
                    Exception = ResultExceptionCapture.Capture(ex, _options, _logger),
                    CompletedAt = _timeProvider.GetUtcNow(),
                    Duration = _timeProvider.GetUtcNow() - startTime,
                    Retries = message.Retries,
                    Worker = workerName,
                    RetryAfter = retryAfter,
                };

                await RecordOutcomeAsync(retryResult, cancellationToken).ConfigureAwait(false);

                await _signalDispatcher
                    .DispatchAsync(
                        new TaskRetrySignal
                        {
                            TaskId = message.Id,
                            TaskName = message.Task,
                            Timestamp = _timeProvider.GetUtcNow(),
                            Exception = ex,
                            RetryCount = message.Retries,
                            Countdown = retryAfter,
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
                            State = TaskState.Retry,
                            Duration = retryResult.Duration,
                            Worker = workerName,
                        },
                        cancellationToken
                    )
                    .ConfigureAwait(false);

                return retryResult;
            }

            _logger.LogError(ex, "Task {TaskId} failed", message.Id);

            var taskResult = new TaskResult
            {
                TaskId = message.Id,
                State = TaskState.Failure,
                Exception = ResultExceptionCapture.Capture(ex, _options, _logger),
                CompletedAt = _timeProvider.GetUtcNow(),
                Duration = _timeProvider.GetUtcNow() - startTime,
                Retries = message.Retries,
                Worker = workerName,
            };

            await RecordOutcomeAsync(taskResult, cancellationToken).ConfigureAwait(false);

            // Dispatch failure signals
            await _signalDispatcher
                .DispatchAsync(
                    new TaskFailureSignal
                    {
                        TaskId = message.Id,
                        TaskName = message.Task,
                        Timestamp = _timeProvider.GetUtcNow(),
                        Exception = ex,
                        RetryCount = message.Retries,
                        Duration = taskResult.Duration,
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
                        State = TaskState.Failure,
                        Duration = taskResult.Duration,
                        Worker = workerName,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            return taskResult;
        }
        finally
        {
            _revocationManager.UnregisterTask(message.Id);
        }
    }
}
