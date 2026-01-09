using DotCelery.Core.Exceptions;
using DotCelery.Core.TimeLimits;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Enforces time limits on task execution.
/// </summary>
public sealed class TimeLimitEnforcer
{
    private readonly ILogger<TimeLimitEnforcer> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="TimeLimitEnforcer"/> class.
    /// </summary>
    /// <param name="logger">The logger.</param>
    public TimeLimitEnforcer(ILogger<TimeLimitEnforcer> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Executes a task with time limit enforcement.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    /// <param name="taskId">The task ID.</param>
    /// <param name="policy">The time limit policy.</param>
    /// <param name="taskExecution">The task execution function.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The task result.</returns>
    public async Task<T> ExecuteWithTimeLimitsAsync<T>(
        string taskId,
        TimeLimitPolicy? policy,
        Func<CancellationToken, Task<T>> taskExecution,
        CancellationToken cancellationToken
    )
    {
        if (policy is null || !policy.HasLimits)
        {
            return await taskExecution(cancellationToken).ConfigureAwait(false);
        }

        // Create a CTS that will be cancelled on any time limit (soft or hard)
        // This ensures the task is always stopped when limits are exceeded
        using var executionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        using var softLimitCts = policy.SoftLimit.HasValue ? new CancellationTokenSource() : null;

        // Set up hard limit timeout
        if (policy.HardLimit.HasValue)
        {
            executionCts.CancelAfter(policy.HardLimit.Value);
        }

        // Set up soft limit timeout
        if (softLimitCts is not null && policy.SoftLimit.HasValue)
        {
            softLimitCts.CancelAfter(policy.SoftLimit.Value);
        }

        var executionToken = executionCts.Token;
        var softLimitExceeded = false;

        try
        {
            // Create a task that monitors the soft limit
            if (softLimitCts is not null && policy.SoftLimit.HasValue)
            {
                var executionTask = taskExecution(executionToken);

                // Wait for either the task to complete or soft limit to trigger
                var softLimitTask = WaitForSoftLimitAsync(softLimitCts.Token);

                var completedTask = await Task.WhenAny(executionTask, softLimitTask)
                    .ConfigureAwait(false);

                if (completedTask == softLimitTask && !executionTask.IsCompleted)
                {
                    // Soft limit triggered - cancel the execution and throw exception
                    softLimitExceeded = true;

                    _logger.LogWarning(
                        "Task {TaskId} exceeded soft time limit of {SoftLimit}",
                        taskId,
                        policy.SoftLimit.Value
                    );

                    // Cancel the execution to stop the running task
                    await executionCts.CancelAsync().ConfigureAwait(false);

                    throw new SoftTimeLimitExceededException(taskId, policy.SoftLimit.Value);
                }

                return await executionTask.ConfigureAwait(false);
            }

            return await taskExecution(executionToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (executionCts.IsCancellationRequested
                && !cancellationToken.IsCancellationRequested
                && !softLimitExceeded
            )
        {
            // Hard limit exceeded (not soft limit and not external cancellation)
            _logger.LogWarning(
                "Task {TaskId} exceeded hard time limit of {HardLimit}",
                taskId,
                policy.HardLimit
            );

            throw new TimeoutException(
                $"Task {taskId} exceeded hard time limit of {policy.HardLimit?.TotalSeconds} seconds."
            );
        }
    }

    /// <summary>
    /// Executes a task with time limit enforcement (void return).
    /// </summary>
    /// <param name="taskId">The task ID.</param>
    /// <param name="policy">The time limit policy.</param>
    /// <param name="taskExecution">The task execution function.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task ExecuteWithTimeLimitsAsync(
        string taskId,
        TimeLimitPolicy? policy,
        Func<CancellationToken, Task> taskExecution,
        CancellationToken cancellationToken
    )
    {
        await ExecuteWithTimeLimitsAsync<object?>(
                taskId,
                policy,
                async ct =>
                {
                    await taskExecution(ct).ConfigureAwait(false);
                    return null;
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    private static async Task WaitForSoftLimitAsync(CancellationToken softLimitToken)
    {
        try
        {
            await Task.Delay(Timeout.Infinite, softLimitToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Soft limit token cancelled - this is expected
        }
    }
}
