using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.CanvasExample.Tasks;

/// <summary>
/// Input of the chord callback. A chord callback receives the input from its signature; the
/// header task IDs are on the chord result the client gets back.
/// </summary>
public sealed record AggregateInput
{
    public required string Label { get; init; }
    public required int ExpectedCount { get; init; }
}

/// <summary>
/// Result of the chord callback.
/// </summary>
public sealed record AggregateResult
{
    public required string Label { get; init; }
    public required int ExpectedCount { get; init; }
    public required DateTimeOffset CompletedAt { get; init; }
}

/// <summary>
/// Callback of the chord: runs once after every header task has finished.
/// </summary>
public sealed class AggregateResultsTask(ILogger<AggregateResultsTask> logger)
    : ITask<AggregateInput, AggregateResult>
{
    public static string TaskName => "canvas.aggregate_results";

    public Task<AggregateResult> ExecuteAsync(
        AggregateInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        logger.LogInformation(
            "[Task {TaskId}] Chord callback {Label} ran after {Count} header tasks",
            context.TaskId,
            input.Label,
            input.ExpectedCount
        );

        return Task.FromResult(
            new AggregateResult
            {
                Label = input.Label,
                ExpectedCount = input.ExpectedCount,
                CompletedAt = DateTimeOffset.UtcNow,
            }
        );
    }
}
