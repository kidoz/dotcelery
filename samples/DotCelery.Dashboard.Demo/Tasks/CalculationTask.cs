using DotCelery.Core.Abstractions;

namespace DotCelery.Dashboard.Demo.Tasks;

public sealed record CalculationInput
{
    public required int A { get; init; }
    public required int B { get; init; }
    public required string Operation { get; init; }
}

public sealed record CalculationResult
{
    public required int Result { get; init; }
    public required string Expression { get; init; }
}

public sealed class CalculationTask(ILogger<CalculationTask> logger)
    : ITask<CalculationInput, CalculationResult>
{
    public static string TaskName => "demo.math.calculate";

    public async Task<CalculationResult> ExecuteAsync(
        CalculationInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        await Task.Delay(TimeSpan.FromMilliseconds(Random.Shared.Next(50, 250)), cancellationToken);

        var result = input.Operation switch
        {
            "+" => input.A + input.B,
            "-" => input.A - input.B,
            "*" => input.A * input.B,
            "/" when input.B != 0 => input.A / input.B,
            "/" => throw new DivideByZeroException(),
            _ => throw new ArgumentException($"Unknown operation: {input.Operation}"),
        };

        var expression = $"{input.A} {input.Operation} {input.B} = {result}";
        logger.LogInformation("[Task {TaskId}] {Expression}", context.TaskId, expression);

        return new CalculationResult { Result = result, Expression = expression };
    }
}
