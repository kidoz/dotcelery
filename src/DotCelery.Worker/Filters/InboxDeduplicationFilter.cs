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
/// Note: For true exactly-once semantics, the inbox store, task execution, and result
/// storage should ideally be in a single transaction. This implementation provides
/// at-most-once semantics for the inbox check and marks messages processed after
/// successful execution.
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

        // Only mark as processed if execution was successful
        if (context.TaskResult?.State == TaskState.Success)
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
