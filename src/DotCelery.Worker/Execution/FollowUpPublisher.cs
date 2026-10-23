using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Publishes the follow-up a settled task carries: the next chain step, a linked success
/// callback, or a linked error callback.
/// </summary>
/// <remarks>
/// The message is only acknowledged once its follow-up is published, so a failure returns the
/// original message to the broker and the follow-up is published again instead of being lost.
/// </remarks>
internal sealed class FollowUpPublisher(
    IMessageBroker broker,
    WorkerMessageSettler settler,
    ILogger logger,
    TimeProvider timeProvider,
    WorkerOptions options
)
{
    /// <summary>
    /// Publishes the next step of the chain the task belongs to, with the task's result as its
    /// input.
    /// </summary>
    /// <returns><c>true</c> when the caller may acknowledge the message.</returns>
    public Task<bool> ContinueChainAsync(
        BrokerMessage message,
        TaskResult result,
        CancellationToken cancellationToken
    )
    {
        if (message.Message.Chain is not { Count: > 0 } steps)
        {
            return Task.FromResult(true);
        }

        var step = steps[0];

        return PublishFollowUpAsync(
            message,
            step.Signature,
            step.TaskId,
            result.Result ?? step.Signature.Args ?? [],
            result.ContentType ?? message.Message.ContentType,
            chain: steps.Count > 1 ? [.. steps.Skip(1)] : null,
            cancellationToken
        );
    }

    /// <summary>
    /// Publishes the linked callback of a successful task with the task's result as the
    /// callback's input.
    /// </summary>
    /// <returns><c>true</c> when the caller may acknowledge the message.</returns>
    public Task<bool> ContinueWithLinkAsync(
        BrokerMessage message,
        TaskResult result,
        CancellationToken cancellationToken
    )
    {
        if (message.Message.Link is not { } link)
        {
            return Task.FromResult(true);
        }

        return PublishFollowUpAsync(
            message,
            link,
            $"{message.Message.Id}:link",
            result.Result ?? link.Args ?? [],
            result.ContentType ?? message.Message.ContentType,
            chain: null,
            cancellationToken
        );
    }

    /// <summary>
    /// Publishes the error callback of a failed task with a <see cref="TaskErrorInfo"/> payload
    /// as the callback's input.
    /// </summary>
    /// <returns><c>true</c> when the caller may acknowledge the message.</returns>
    public Task<bool> ContinueWithErrorLinkAsync(
        BrokerMessage message,
        TaskResult result,
        CancellationToken cancellationToken
    )
    {
        if (message.Message.LinkError is not { } errorLink)
        {
            return Task.FromResult(true);
        }

        var error = new TaskErrorInfo
        {
            TaskId = message.Message.Id,
            TaskName = message.Message.Task,
            ErrorType = result.Exception?.Type,
            ErrorMessage = result.Exception?.Message,
        };

        return PublishFollowUpAsync(
            message,
            errorLink,
            $"{message.Message.Id}:link-error",
            JsonSerializer.SerializeToUtf8Bytes(error, DotCeleryJsonContext.Default.TaskErrorInfo),
            "application/json",
            chain: null,
            cancellationToken
        );
    }

    private async Task<bool> PublishFollowUpAsync(
        BrokerMessage message,
        Signature followUp,
        string followUpId,
        byte[] args,
        string contentType,
        IReadOnlyList<ChainStep>? chain,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await broker
                .PublishAsync(
                    TaskMessageFactory.ForFollowUp(
                        message.Message,
                        followUpId,
                        followUp,
                        args,
                        contentType,
                        timeProvider.GetUtcNow(),
                        followUp.Queue,
                        chain
                    ),
                    cancellationToken
                )
                .ConfigureAwait(false);

            logger.LogDebug(
                "Task {TaskId} published follow-up {FollowUpId} for task {FollowUpTaskName}",
                message.Message.Id,
                followUpId,
                followUp.TaskName
            );

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(
                ex,
                "Failed to publish follow-up {FollowUpId} of task {TaskId}; returning the message to the broker",
                followUpId,
                message.Message.Id
            );

            await settler
                .ReturnAfterFailureAsync(
                    message,
                    options.InfrastructureFailureRequeueDelay,
                    cancellationToken
                )
                .ConfigureAwait(false);

            return false;
        }
    }
}
