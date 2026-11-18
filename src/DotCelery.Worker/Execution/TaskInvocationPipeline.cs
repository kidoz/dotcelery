using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Filters;
using DotCelery.Core.TimeLimits;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;

namespace DotCelery.Worker.Execution;

internal sealed class TaskInvocationPipeline(
    TaskFilterPipeline filterPipeline,
    TimeLimitEnforcer? timeLimitEnforcer,
    TenantDeliveryValidator tenant
)
{
    private readonly CompiledTaskInvoker _invoker = new();

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "The compiled invoker is only reached for a task registered without a generated invoker; a Native AOT application is refused below instead."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "The compiled invoker compiles an expression tree, which is refused below when the application targets Native AOT."
    )]
    private Task<object?> InvokeTaskAsync(
        object task,
        object? input,
        ITaskContext context,
        TaskRegistration registration,
        CancellationToken cancellationToken
    )
    {
        // A generated registration carries a typed adapter, so invoking it needs neither
        // reflection nor expression compilation; the compiled invoker remains for the
        // registrations that came from a reflective scan or a manual registration
        if (registration.Invoker is { } invoker)
        {
            return invoker.InvokeAsync(task, input, context, cancellationToken);
        }

        if (!RuntimeFeature.IsDynamicCodeSupported)
        {
            throw new InvalidOperationException(
                $"Task '{registration.TaskName}' has no invoker and the application targets Native AOT, "
                    + "where reflection-based invocation is unavailable. Register the task through the "
                    + "source-generated registration, which supplies a typed invoker, or pass one to "
                    + "AddTask<TTask>(taskName, invoker)."
            );
        }

        return _invoker.InvokeAsync(task, input, context, registration, cancellationToken);
    }

    public async Task<(
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
        BrokerMessage brokerMessage,
        Func<Task> onStarting,
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
            BrokerMessage = brokerMessage,
            Input = input,
            TaskType = registration.TaskType,
            TaskContext = context,
            ServiceProvider = context.ScopedServiceProvider,
            RetryCount = context.RetryCount,
        };

        try
        {
            // Execute OnExecuting filters
            var shouldContinue = await filterPipeline
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

            using var executionCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                PartitionedExecutionFilter.LeaseLostToken(executingContext)
            );
            var executionToken = executionCts.Token;
            executionToken.ThrowIfCancellationRequested();
            await onStarting().ConfigureAwait(false);

            // The tenant context is set here rather than in a filter: a value written to an
            // AsyncLocal inside an awaited async method (the filter pipeline) does not flow back to
            // its caller, so a filter that sets it is never seen by the task.
            using var tenantScope = tenant.EnterScope(context.Message);

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
                        executionToken
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

                var handled = await filterPipeline
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

            await filterPipeline
                .ExecuteOnExecutedAsync(filters, executedContext, cancellationToken)
                .ConfigureAwait(false);

            // Use potentially modified values from executed context
            if (executedContext.Exception is not null)
            {
                throw executedContext.Exception;
            }

            return (executedContext.Result, stopwatch.Elapsed, false, null);
        }
        finally
        {
            for (var i = filters.Count - 1; i >= 0; i--)
            {
                if (filters[i].TaskFilter is IExecutionResourceFilter resourceFilter)
                    await resourceFilter.ReleaseAsync(executingContext).ConfigureAwait(false);
            }
        }
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
        if (timeLimitEnforcer is null || policy is null || !policy.HasLimits)
        {
            return await InvokeTaskAsync(task, input, context, registration, cancellationToken)
                .ConfigureAwait(false);
        }

        // Execute with time limits
        return await timeLimitEnforcer
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
