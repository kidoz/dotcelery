using System.Diagnostics;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using DotCelery.Core.Signals;
using DotCelery.Core.TimeLimits;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Executes tasks using compiled delegates and DI.
/// </summary>
public sealed class TaskExecutor
{
    private readonly TaskRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly IMessageSerializer _serializer;
    private readonly IResultBackend _resultBackend;
    private readonly RevocationManager _revocationManager;
    private readonly TaskFilterPipeline _filterPipeline;
    private readonly ITaskSignalDispatcher _signalDispatcher;
    private readonly TimeLimitEnforcer? _timeLimitEnforcer;
    private readonly IRateLimiter? _rateLimiter;
    private readonly CompiledTaskInvoker _taskInvoker;
    private readonly WorkerOptions _options;
    private readonly ILogger<TaskExecutor> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutor"/> class.
    /// </summary>
    public TaskExecutor(
        TaskRegistry registry,
        IServiceProvider serviceProvider,
        IMessageSerializer serializer,
        IResultBackend resultBackend,
        RevocationManager revocationManager,
        TaskFilterPipeline filterPipeline,
        IOptions<WorkerOptions> options,
        ILogger<TaskExecutor> logger,
        ITaskSignalDispatcher? signalDispatcher = null,
        IRateLimiter? rateLimiter = null,
        TimeLimitEnforcer? timeLimitEnforcer = null
    )
    {
        _registry = registry;
        _serviceProvider = serviceProvider;
        _serializer = serializer;
        _resultBackend = resultBackend;
        _revocationManager = revocationManager;
        _filterPipeline = filterPipeline;
        _signalDispatcher = signalDispatcher ?? NullTaskSignalDispatcher.Instance;
        _timeLimitEnforcer = timeLimitEnforcer;
        _rateLimiter = rateLimiter;
        _taskInvoker = new CompiledTaskInvoker();
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// Executes a task from a broker message.
    /// </summary>
    /// <param name="brokerMessage">The broker message.</param>
    /// <param name="workerName">The worker name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The task result.</returns>
    public async Task<TaskResult> ExecuteAsync(
        BrokerMessage brokerMessage,
        string? workerName,
        CancellationToken cancellationToken
    )
    {
        var message = brokerMessage.Message;
        var startTime = DateTimeOffset.UtcNow;

        var registration = _registry.GetTask(message.Task);
        if (registration is null)
        {
            _logger.LogError("Unknown task: {TaskName}", message.Task);
            throw new UnknownTaskException(message.Task);
        }

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

        // Update state to Started
        await _resultBackend
            .UpdateStateAsync(message.Id, TaskState.Started, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        try
        {
            await using var scope = _serviceProvider.CreateAsyncScope();

            // Resolve task instance
            var task = scope.ServiceProvider.GetRequiredService(registration.TaskType);

            // Deserialize input
            object? input = null;
            if (registration.InputType is not null)
            {
                input = _serializer.Deserialize(message.Args, registration.InputType);
            }

            // Create execution context
            var context = new TaskExecutionContext(
                message: message,
                serviceProvider: scope.ServiceProvider,
                resultBackend: _resultBackend,
                signalDispatcher: _signalDispatcher,
                workerName: workerName
            );

            // Get filters for this task
            var filters = _filterPipeline.GetFilters(registration, scope);

            // Dispatch TaskPreRun signal
            await _signalDispatcher
                .DispatchAsync(
                    new TaskPreRunSignal
                    {
                        TaskId = message.Id,
                        TaskName = message.Task,
                        Timestamp = DateTimeOffset.UtcNow,
                        Input = input,
                        RetryCount = message.Retries,
                        Worker = workerName,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            // Execute task with filters
            var (result, duration, requeueRequested, requeueDelay) = await ExecuteWithFiltersAsync(
                    task,
                    input,
                    context,
                    registration,
                    filters,
                    taskToken
                )
                .ConfigureAwait(false);

            // Check if requeue was requested by a filter
            if (requeueRequested)
            {
                _logger.LogDebug("Task {TaskId} filter requested requeue", message.Id);

                var requeueResult = new TaskResult
                {
                    TaskId = message.Id,
                    State = TaskState.Requeued,
                    CompletedAt = DateTimeOffset.UtcNow,
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
                            Timestamp = DateTimeOffset.UtcNow,
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
                // Store the result if it's a terminal state
                if (
                    filterProvidedResult.State
                    is TaskState.Success
                        or TaskState.Rejected
                        or TaskState.Failure
                )
                {
                    await _resultBackend
                        .StoreResultAsync(
                            filterProvidedResult,
                            cancellationToken: cancellationToken
                        )
                        .ConfigureAwait(false);
                }

                _logger.LogInformation(
                    "Task {TaskId} completed with filter-provided result: {State}",
                    message.Id,
                    filterProvidedResult.State
                );

                return filterProvidedResult;
            }

            var taskResult = new TaskResult
            {
                TaskId = message.Id,
                State = TaskState.Success,
                Result = result is not null ? _serializer.Serialize(result) : null,
                ContentType = _serializer.ContentType,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = duration,
                Retries = message.Retries,
                Worker = workerName,
            };

            await _resultBackend
                .StoreResultAsync(taskResult, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Task {TaskId} completed successfully in {Duration}ms",
                message.Id,
                duration.TotalMilliseconds
            );

            // Dispatch success signals
            await _signalDispatcher
                .DispatchAsync(
                    new TaskSuccessSignal
                    {
                        TaskId = message.Id,
                        TaskName = message.Task,
                        Timestamp = DateTimeOffset.UtcNow,
                        Result = result,
                        Duration = duration,
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
                        Timestamp = DateTimeOffset.UtcNow,
                        State = TaskState.Success,
                        Duration = duration,
                        Result = result,
                        Worker = workerName,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            return taskResult;
        }
        catch (OperationCanceledException)
            when (taskToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
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
        catch (RetryException ex)
        {
            _logger.LogInformation(
                "Task {TaskId} requested retry: {Message}",
                message.Id,
                ex.Message
            );

            var taskResult = new TaskResult
            {
                TaskId = message.Id,
                State = TaskState.Retry,
                Exception = TaskExceptionInfo.FromException(ex.InnerException ?? ex),
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = DateTimeOffset.UtcNow - startTime,
                Retries = message.Retries,
                Worker = workerName,
                RetryAfter = ex.Countdown,
            };

            await _resultBackend
                .StoreResultAsync(taskResult, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            // Dispatch retry signals
            await _signalDispatcher
                .DispatchAsync(
                    new TaskRetrySignal
                    {
                        TaskId = message.Id,
                        TaskName = message.Task,
                        Timestamp = DateTimeOffset.UtcNow,
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
                        Timestamp = DateTimeOffset.UtcNow,
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

            var taskResult = new TaskResult
            {
                TaskId = message.Id,
                State = TaskState.Rejected,
                Exception = TaskExceptionInfo.FromException(ex),
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = DateTimeOffset.UtcNow - startTime,
                Retries = message.Retries,
                Worker = workerName,
            };

            await _resultBackend
                .StoreResultAsync(taskResult, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            // Dispatch rejected signals
            await _signalDispatcher
                .DispatchAsync(
                    new TaskRejectedSignal
                    {
                        TaskId = message.Id,
                        TaskName = message.Task,
                        Timestamp = DateTimeOffset.UtcNow,
                        Reason = ex.Message,
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
                        Timestamp = DateTimeOffset.UtcNow,
                        State = TaskState.Rejected,
                        Duration = taskResult.Duration,
                        Worker = workerName,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            return taskResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Task {TaskId} failed", message.Id);

            var taskResult = new TaskResult
            {
                TaskId = message.Id,
                State = TaskState.Failure,
                Exception = TaskExceptionInfo.FromException(ex),
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = DateTimeOffset.UtcNow - startTime,
                Retries = message.Retries,
                Worker = workerName,
            };

            await _resultBackend
                .StoreResultAsync(taskResult, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            // Dispatch failure signals
            await _signalDispatcher
                .DispatchAsync(
                    new TaskFailureSignal
                    {
                        TaskId = message.Id,
                        TaskName = message.Task,
                        Timestamp = DateTimeOffset.UtcNow,
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
                        Timestamp = DateTimeOffset.UtcNow,
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
            CompletedAt = DateTimeOffset.UtcNow,
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
            CompletedAt = DateTimeOffset.UtcNow,
            Duration = DateTimeOffset.UtcNow - startTime,
            Retries = message.Retries,
            Worker = workerName,
        };

        await _resultBackend
            .StoreResultAsync(taskResult, cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        // Dispatch revoked signals
        await _signalDispatcher
            .DispatchAsync(
                new TaskRevokedSignal
                {
                    TaskId = message.Id,
                    TaskName = message.Task,
                    Timestamp = DateTimeOffset.UtcNow,
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
                    Timestamp = DateTimeOffset.UtcNow,
                    State = TaskState.Revoked,
                    Duration = taskResult.Duration,
                    Worker = workerName,
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        return taskResult;
    }

    private Task<object?> InvokeTaskAsync(
        object task,
        object? input,
        ITaskContext context,
        TaskRegistration registration,
        CancellationToken cancellationToken
    )
    {
        // Use compiled delegate invoker for type-safe and efficient task invocation.
        // This validates method signatures at compile time and caches delegates,
        // avoiding repeated reflection overhead and ensuring correct method binding.
        return _taskInvoker.InvokeAsync(task, input, context, registration, cancellationToken);
    }

    private async Task<(
        object? Result,
        TimeSpan Duration,
        bool RequeueRequested,
        TimeSpan? RequeueDelay
    )> ExecuteWithFiltersAsync(
        object task,
        object? input,
        TaskExecutionContext context,
        TaskRegistration registration,
        IReadOnlyList<ResolvedFilter> filters,
        CancellationToken cancellationToken
    )
    {
        var stopwatch = Stopwatch.StartNew();
        object? result = null;
        Exception? exception = null;

        // Create executing context
        var executingContext = new TaskExecutingContext
        {
            TaskId = context.TaskId,
            TaskName = context.TaskName,
            Message = context.Message,
            Input = input,
            TaskType = registration.TaskType,
            TaskContext = context,
            ServiceProvider = context.ScopedServiceProvider,
            RetryCount = context.RetryCount,
        };

        // Execute OnExecuting filters
        var shouldContinue = await _filterPipeline
            .ExecuteOnExecutingAsync(filters, executingContext, cancellationToken)
            .ConfigureAwait(false);

        if (!shouldContinue)
        {
            // Filter requested to skip execution
            stopwatch.Stop();

            // If a full TaskResult was provided, return it via the SkipResult property
            // This is used by security and deduplication filters to set specific states
            if (executingContext.SkipResult is not null)
            {
                return (
                    executingContext.SkipResult,
                    stopwatch.Elapsed,
                    executingContext.RequeueMessage,
                    executingContext.RequeueDelay
                );
            }

            return (
                executingContext.Result,
                stopwatch.Elapsed,
                executingContext.RequeueMessage,
                executingContext.RequeueDelay
            );
        }

        // Execute the task with time limit enforcement
        try
        {
            result = await ExecuteWithTimeLimitsAsync(
                    context.TaskId,
                    registration.TimeLimitPolicy,
                    task,
                    input,
                    context,
                    registration,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            exception = ex;

            // Execute exception filters
            var exceptionContext = new TaskExceptionContext
            {
                TaskId = context.TaskId,
                TaskName = context.TaskName,
                Input = input,
                TaskType = registration.TaskType,
                TaskContext = context,
                ServiceProvider = context.ScopedServiceProvider,
                Exception = ex,
                Properties = executingContext.Properties,
            };

            var handled = await _filterPipeline
                .ExecuteOnExceptionAsync(filters, exceptionContext, cancellationToken)
                .ConfigureAwait(false);

            if (handled || exceptionContext.ExceptionHandled)
            {
                exception = null;
                result = exceptionContext.Result;
            }
        }

        stopwatch.Stop();

        // Execute OnExecuted filters
        var executedContext = new TaskExecutedContext
        {
            TaskId = context.TaskId,
            TaskName = context.TaskName,
            Message = context.Message,
            Input = input,
            TaskType = registration.TaskType,
            TaskContext = context,
            ServiceProvider = context.ScopedServiceProvider,
            Duration = stopwatch.Elapsed,
            Result = result,
            Exception = exception,
            Properties = executingContext.Properties,
        };

        await _filterPipeline
            .ExecuteOnExecutedAsync(filters, executedContext, cancellationToken)
            .ConfigureAwait(false);

        // Use potentially modified values from executed context
        if (executedContext.Exception is not null)
        {
            throw executedContext.Exception;
        }

        return (executedContext.Result, stopwatch.Elapsed, false, null);
    }

    private async Task<object?> ExecuteWithTimeLimitsAsync(
        string taskId,
        TimeLimitPolicy? policy,
        object task,
        object? input,
        ITaskContext context,
        TaskRegistration registration,
        CancellationToken cancellationToken
    )
    {
        // If no enforcer or no policy, execute directly
        if (_timeLimitEnforcer is null || policy is null || !policy.HasLimits)
        {
            return await InvokeTaskAsync(task, input, context, registration, cancellationToken)
                .ConfigureAwait(false);
        }

        // Execute with time limits
        return await _timeLimitEnforcer
            .ExecuteWithTimeLimitsAsync(
                taskId,
                policy,
                async ct =>
                    await InvokeTaskAsync(task, input, context, registration, ct)
                        .ConfigureAwait(false),
                cancellationToken
            )
            .ConfigureAwait(false);
    }
}
