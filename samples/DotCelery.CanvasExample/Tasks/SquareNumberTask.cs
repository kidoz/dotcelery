using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.CanvasExample.Tasks;

/// <summary>
/// Input for squaring a number.
/// </summary>
public sealed record SquareInput
{
    public required int Value { get; init; }
    public required int DelayMs { get; init; }
}

/// <summary>
/// Result of squaring a number, with its timing so overlapping executions are visible.
/// </summary>
public sealed record SquareResult
{
    public required int Value { get; init; }
    public required int Square { get; init; }
    public required DateTimeOffset StartedAt { get; init; }
    public required DateTimeOffset CompletedAt { get; init; }
}

/// <summary>
/// A short task used by groups and chords to show parallel execution.
/// </summary>
public sealed class SquareNumberTask(ILogger<SquareNumberTask> logger)
    : ITask<SquareInput, SquareResult>
{
    public static string TaskName => "canvas.square_number";

    public async Task<SquareResult> ExecuteAsync(
        SquareInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        var startedAt = DateTimeOffset.UtcNow;

        await Task.Delay(TimeSpan.FromMilliseconds(input.DelayMs), cancellationToken);

        var completedAt = DateTimeOffset.UtcNow;

        logger.LogInformation(
            "[Task {TaskId}] Squared {Value} after {Delay} ms",
            context.TaskId,
            input.Value,
            input.DelayMs
        );

        return new SquareResult
        {
            Value = input.Value,
            Square = input.Value * input.Value,
            StartedAt = startedAt,
            CompletedAt = completedAt,
        };
    }
}
