using DotCelery.Core.Abstractions;

namespace DotCelery.Dashboard.Demo.Tasks;

public sealed record FlakyInput
{
    public required int FailureRatePercent { get; init; }
}

public sealed record FlakyResult
{
    public required string Status { get; init; }
}

public sealed class FlakyTask(ILogger<FlakyTask> logger) : ITask<FlakyInput, FlakyResult>
{
    public static string TaskName => "demo.flaky";

    public async Task<FlakyResult> ExecuteAsync(
        FlakyInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        await Task.Delay(
            TimeSpan.FromMilliseconds(Random.Shared.Next(100, 400)),
            cancellationToken
        );

        if (Random.Shared.Next(100) < input.FailureRatePercent)
        {
            logger.LogWarning("[Task {TaskId}] Simulating failure", context.TaskId);
            throw new InvalidOperationException("Simulated downstream failure");
        }

        return new FlakyResult { Status = "ok" };
    }
}
