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
/// The filter only skips duplicates. A successful message is marked as processed by the
/// executor's outcome recording (<see cref="DotCelery.Core.Storage.Stores.OutcomeRecorder"/>),
/// which stores the result and the record in one storage transaction when the stores can share
/// one, and after the result otherwise. A message that fails is not marked, so it is executed
/// again when it is redelivered.
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
    /// <remarks>
    /// Nothing to do: the message is marked as processed by the executor once the outcome is
    /// stored, in the same transaction when the result backend and the inbox store can share one.
    /// </remarks>
    public ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    ) => ValueTask.CompletedTask;
}
