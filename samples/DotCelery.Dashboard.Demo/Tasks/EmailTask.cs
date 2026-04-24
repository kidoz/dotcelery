using DotCelery.Core.Abstractions;

namespace DotCelery.Dashboard.Demo.Tasks;

public sealed record EmailInput
{
    public required string To { get; init; }
    public required string Subject { get; init; }
}

public sealed record EmailResult
{
    public required string MessageId { get; init; }
    public required DateTimeOffset SentAt { get; init; }
}

public sealed class EmailTask(ILogger<EmailTask> logger) : ITask<EmailInput, EmailResult>
{
    public static string TaskName => "demo.email.send";

    public async Task<EmailResult> ExecuteAsync(
        EmailInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        logger.LogInformation("[Task {TaskId}] Sending email to {To}", context.TaskId, input.To);

        await Task.Delay(
            TimeSpan.FromMilliseconds(Random.Shared.Next(200, 800)),
            cancellationToken
        );

        return new EmailResult
        {
            MessageId = Guid.NewGuid().ToString("N"),
            SentAt = DateTimeOffset.UtcNow,
        };
    }
}
