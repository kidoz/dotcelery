using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.Demo.Tasks;

/// <summary>
/// Input for a long running task.
/// </summary>
public sealed record LongRunningInput
{
    public required int Iterations { get; init; }
    public required int DelayPerIterationMs { get; init; }
}

/// <summary>
/// Result of a long running task.
/// </summary>
public sealed record LongRunningResult
{
    public required int CompletedIterations { get; init; }
    public required TimeSpan TotalDuration { get; init; }
}

/// <summary>
/// Task that runs for an extended period with progress updates.
/// Useful for demonstrating task cancellation.
/// </summary>
public sealed class LongRunningTask(ILogger<LongRunningTask> logger)
    : ITask<LongRunningInput, LongRunningResult>
{
    public static string TaskName => "demo.longrunning";

    public async Task<LongRunningResult> ExecuteAsync(
        LongRunningInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        logger.LogInformation(
            "[Task {TaskId}] Starting long running task with {Iterations} iterations",
            context.TaskId,
            input.Iterations
        );

        var startTime = DateTimeOffset.UtcNow;
        var completedIterations = 0;

        for (var i = 0; i < input.Iterations; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Task.Delay(
                TimeSpan.FromMilliseconds(input.DelayPerIterationMs),
                cancellationToken
            );
            completedIterations++;

            logger.LogInformation(
                "[Task {TaskId}] Progress: {Completed}/{Total}",
                context.TaskId,
                completedIterations,
                input.Iterations
            );
        }

        var duration = DateTimeOffset.UtcNow - startTime;

        logger.LogInformation(
            "[Task {TaskId}] Long running task completed in {Duration}",
            context.TaskId,
            duration
        );

        return new LongRunningResult
        {
            CompletedIterations = completedIterations,
            TotalDuration = duration,
        };
    }
}
