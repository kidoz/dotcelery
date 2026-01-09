using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.Demo.Tasks;

/// <summary>
/// Input for a calculation operation.
/// </summary>
public sealed record CalculationInput
{
    public required int A { get; init; }
    public required int B { get; init; }
    public required string Operation { get; init; }
}

/// <summary>
/// Result of a calculation.
/// </summary>
public sealed record CalculationResult
{
    public required int Result { get; init; }
    public required string Expression { get; init; }
}

/// <summary>
/// Task that performs simple calculations.
/// </summary>
public sealed class CalculationTask(ILogger<CalculationTask> logger)
    : ITask<CalculationInput, CalculationResult>
{
    public static string TaskName => "math.calculate";

    public async Task<CalculationResult> ExecuteAsync(
        CalculationInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        logger.LogInformation(
            "[Task {TaskId}] Calculating {A} {Op} {B}",
            context.TaskId,
            input.A,
            input.Operation,
            input.B
        );

        // Simulate some work
        await Task.Delay(
            TimeSpan.FromMilliseconds(Random.Shared.Next(100, 500)),
            cancellationToken
        );

        var result = input.Operation.ToLowerInvariant() switch
        {
            "add" or "+" => input.A + input.B,
            "subtract" or "-" => input.A - input.B,
            "multiply" or "*" => input.A * input.B,
            "divide" or "/" when input.B != 0 => input.A / input.B,
            "divide" or "/" => throw new DivideByZeroException("Cannot divide by zero"),
            _ => throw new ArgumentException($"Unknown operation: {input.Operation}"),
        };

        var expression = $"{input.A} {input.Operation} {input.B} = {result}";

        logger.LogInformation(
            "[Task {TaskId}] Calculation complete: {Expression}",
            context.TaskId,
            expression
        );

        return new CalculationResult { Result = result, Expression = expression };
    }
}
