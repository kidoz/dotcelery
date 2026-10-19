using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.CanvasExample.Tasks;

/// <summary>
/// Result of summing the generated numbers.
/// </summary>
public sealed record SumResult
{
    public required long Sum { get; init; }
    public required int Count { get; init; }
}

/// <summary>
/// Middle step of the chain: receives the previous step's result as its input.
/// </summary>
public sealed class SumNumbersTask(ILogger<SumNumbersTask> logger) : ITask<NumbersResult, SumResult>
{
    public static string TaskName => "canvas.sum_numbers";

    public async Task<SumResult> ExecuteAsync(
        NumbersResult input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

        var sum = input.Values.Sum(value => (long)value);

        logger.LogInformation(
            "[Task {TaskId}] Summed {Count} numbers to {Sum}",
            context.TaskId,
            input.Values.Length,
            sum
        );

        return new SumResult { Sum = sum, Count = input.Values.Length };
    }
}
