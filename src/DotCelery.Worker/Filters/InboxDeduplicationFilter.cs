using DotCelery.Core.Abstractions;
using DotCelery.Core.Filters;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that provides exactly-once message processing using the inbox pattern.
/// Messages that have already been processed are skipped.
/// </summary>
/// <remarks>
/// <para>
/// This filter should be registered with a low order (e.g., -1000) to ensure it runs
/// before other filters, preventing duplicate processing of already-handled messages.
/// </para>
/// <para>
/// A message is marked as processed after it runs successfully, so a redelivery of a message
/// that failed is executed again. For exactly-once semantics the inbox record, the task's
/// effects, and the result must commit together; pass the database transaction to
/// <see cref="IInboxStore.MarkProcessedAsync"/> from the task itself for that.
/// </para>
/// </remarks>
public sealed class InboxDeduplicationFilter : ITaskFilter
{
    private readonly IInboxStore? _inboxStore;
    private readonly ILogger<InboxDeduplicationFilter> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="InboxDeduplicationFilter"/> class.
    /// </summary>
    public InboxDeduplicationFilter(
        ILogger<InboxDeduplicationFilter> logger,
        IInboxStore? inboxStore = null
    )
    {
        _inboxStore = inboxStore;
        _logger = logger;

        if (inboxStore is null)
        {
            _logger.LogWarning(
                "Inbox deduplication is enabled but no IInboxStore is registered, so redelivered "
                    + "messages are processed again. Register an inbox store, for example with UseInbox<T>()."
            );
        }
    }

    /// <summary>
    /// Gets the execution order. Runs early to skip duplicates before other filters.
    /// </summary>
    public int Order => -1000;

    /// <inheritdoc />
    public async ValueTask OnExecutingAsync(
        TaskExecutingContext context,
        CancellationToken cancellationToken
    )
    {
        if (_inboxStore is null)
        {
            // Inbox store not configured, skip deduplication
            return;
        }

        var messageId = context.TaskId;

        var isProcessed = await _inboxStore
            .IsProcessedAsync(messageId, cancellationToken)
            .ConfigureAwait(false);

        if (isProcessed)
        {
            _logger.LogDebug(
                "Message {MessageId} already processed (duplicate), skipping execution",
                messageId
            );

            // Skip execution and set a success result
            context.SkipExecution = true;
            context.SkipResult = new TaskResult
            {
                TaskId = context.TaskId,
                State = TaskState.Success,
                CompletedAt = DateTimeOffset.UtcNow,
                Duration = TimeSpan.Zero,
                Metadata = new Dictionary<string, object>
                {
                    ["deduplicated"] = true,
                    ["originalProcessingTime"] = "unknown",
                },
            };
        }
    }

    /// <inheritdoc />
    public async ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    )
    {
        if (_inboxStore is null)
        {
            return;
        }

        // Mark as processed only after a successful execution. The executor leaves TaskResult
        // unset unless a filter set it, so success is the absence of an exception.
        var succeeded =
            context.TaskResult?.State == TaskState.Success
            || (context.TaskResult is null && context.Exception is null);

        if (succeeded)
        {
            try
            {
                await _inboxStore
                    .MarkProcessedAsync(context.TaskId, transaction: null, cancellationToken)
                    .ConfigureAwait(false);

                _logger.LogDebug(
                    "Marked message {MessageId} as processed in inbox",
                    context.TaskId
                );
            }
            catch (Exception ex)
            {
                // Log but don't fail - the task was already executed successfully
                // A duplicate might slip through, but we prefer at-least-once over at-most-once
                _logger.LogWarning(
                    ex,
                    "Failed to mark message {MessageId} as processed in inbox",
                    context.TaskId
                );
            }
        }
    }
}
