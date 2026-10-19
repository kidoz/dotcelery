using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.CanvasExample.Tasks;

/// <summary>
/// Input for generating a sequence of numbers.
/// </summary>
public sealed record GenerateNumbersInput
{
    public required int Count { get; init; }
    public required int Start { get; init; }
}

/// <summary>
/// The generated numbers, which are the input of the next chain step.
/// </summary>
public sealed record NumbersResult
{
    public required int[] Values { get; init; }
}

/// <summary>
/// First step of the chain: generates a sequence of numbers.
/// </summary>
public sealed class GenerateNumbersTask(ILogger<GenerateNumbersTask> logger)
    : ITask<GenerateNumbersInput, NumbersResult>
{
    public static string TaskName => "canvas.generate_numbers";

    public async Task<NumbersResult> ExecuteAsync(
        GenerateNumbersInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

        var values = Enumerable.Range(input.Start, input.Count).ToArray();

        logger.LogInformation(
            "[Task {TaskId}] Generated {Count} numbers starting at {Start}",
            context.TaskId,
            values.Length,
            input.Start
        );

        return new NumbersResult { Values = values };
    }
}
