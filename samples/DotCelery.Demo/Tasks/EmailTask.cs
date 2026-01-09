using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Logging;

namespace DotCelery.Demo.Tasks;

/// <summary>
/// Input for sending an email.
/// </summary>
public sealed record EmailInput
{
    public required string To { get; init; }
    public required string Subject { get; init; }
    public required string Body { get; init; }
}

/// <summary>
/// Result of sending an email.
/// </summary>
public sealed record EmailResult
{
    public required string MessageId { get; init; }
    public required bool Success { get; init; }
    public required DateTimeOffset SentAt { get; init; }
}

/// <summary>
/// Task that simulates sending an email.
/// </summary>
public sealed class EmailTask(ILogger<EmailTask> logger) : ITask<EmailInput, EmailResult>
{
    public static string TaskName => "email.send";

    public async Task<EmailResult> ExecuteAsync(
        EmailInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        logger.LogInformation(
            "[Task {TaskId}] Sending email to {To} with subject: {Subject}",
            context.TaskId,
            input.To,
            input.Subject
        );

        // Simulate email sending delay
        await Task.Delay(
            TimeSpan.FromMilliseconds(Random.Shared.Next(500, 1500)),
            cancellationToken
        );

        var result = new EmailResult
        {
            MessageId = Guid.NewGuid().ToString("N"),
            Success = true,
            SentAt = DateTimeOffset.UtcNow,
        };

        logger.LogInformation(
            "[Task {TaskId}] Email sent successfully. MessageId: {MessageId}",
            context.TaskId,
            result.MessageId
        );

        return result;
    }
}
