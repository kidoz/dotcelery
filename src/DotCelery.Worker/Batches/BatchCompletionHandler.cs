using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Models;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Batches;

/// <summary>
/// Signal handler that updates batch state when tasks complete, and runs the batch's callback
/// when the last task settles.
/// </summary>
public sealed class BatchCompletionHandler
    : ITaskSignalHandler<TaskSuccessSignal>,
        ITaskSignalHandler<TaskFailureSignal>,
        ITaskSignalHandler<TaskRevokedSignal>,
        ITaskSignalHandler<TaskRejectedSignal>
{
    private readonly IBatchStore _batchStore;
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly ILogger<BatchCompletionHandler> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="BatchCompletionHandler"/> class.
    /// </summary>
    /// <param name="batchStore">The batch store.</param>
    /// <param name="broker">The broker the completion callback is published to.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">The clock for callback messages.</param>
    public BatchCompletionHandler(
        IBatchStore batchStore,
        IMessageBroker broker,
        IMessageSerializer serializer,
        ILogger<BatchCompletionHandler> logger,
        TimeProvider? timeProvider = null
    )
    {
        _batchStore = batchStore;
        _broker = broker;
        _serializer = serializer;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskSuccessSignal signal,
        CancellationToken cancellationToken
    )
    {
        var batchId = await _batchStore
            .GetBatchIdForTaskAsync(signal.TaskId, cancellationToken)
            .ConfigureAwait(false);

        if (batchId is null)
        {
            return;
        }

        var updatedBatch = await _batchStore
            .MarkTaskCompletedAsync(batchId, signal.TaskId, cancellationToken)
            .ConfigureAwait(false);

        if (updatedBatch is not null)
        {
            _logger.LogDebug(
                "Task {TaskId} completed in batch {BatchId} ({Completed}/{Total})",
                signal.TaskId,
                batchId,
                updatedBatch.CompletedCount,
                updatedBatch.TotalTasks
            );

            if (updatedBatch.IsFinished)
            {
                _logger.LogInformation(
                    "Batch {BatchId} finished with state {State}",
                    batchId,
                    updatedBatch.State
                );

                await DispatchCallbackAsync(updatedBatch, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskFailureSignal signal,
        CancellationToken cancellationToken
    )
    {
        await MarkTaskFailedAsync(signal.TaskId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskRevokedSignal signal,
        CancellationToken cancellationToken
    )
    {
        await MarkTaskFailedAsync(signal.TaskId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask HandleAsync(
        TaskRejectedSignal signal,
        CancellationToken cancellationToken
    )
    {
        await MarkTaskFailedAsync(signal.TaskId, cancellationToken).ConfigureAwait(false);
    }

    private async Task MarkTaskFailedAsync(string taskId, CancellationToken cancellationToken)
    {
        var batchId = await _batchStore
            .GetBatchIdForTaskAsync(taskId, cancellationToken)
            .ConfigureAwait(false);

        if (batchId is null)
        {
            return;
        }

        var updatedBatch = await _batchStore
            .MarkTaskFailedAsync(batchId, taskId, cancellationToken)
            .ConfigureAwait(false);

        if (updatedBatch is not null)
        {
            _logger.LogDebug(
                "Task {TaskId} failed in batch {BatchId} ({Failed}/{Total})",
                taskId,
                batchId,
                updatedBatch.FailedCount,
                updatedBatch.TotalTasks
            );

            if (updatedBatch.IsFinished)
            {
                _logger.LogInformation(
                    "Batch {BatchId} finished with state {State}",
                    batchId,
                    updatedBatch.State
                );

                await DispatchCallbackAsync(updatedBatch, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // The callback is claimed in the store first, so it runs once even when the last tasks of a
    // batch settle at the same time in different workers
    private async Task DispatchCallbackAsync(Batch batch, CancellationToken cancellationToken)
    {
        var claimed = await _batchStore
            .TryClaimCallbackAsync(batch.Id, cancellationToken)
            .ConfigureAwait(false);

        if (claimed?.Callback is not { } callback)
        {
            return;
        }

        try
        {
            await _broker
                .PublishAsync(
                    new TaskMessage
                    {
                        Id = callback.TaskId ?? Guid.NewGuid().ToString("N"),
                        Task = callback.TaskName,
                        Args = callback.Args ?? [],
                        ContentType = callback.ContentType ?? _serializer.ContentType,
                        Timestamp = _timeProvider.GetUtcNow(),
                        Queue = callback.Queue,
                        Priority = callback.Priority ?? 0,
                        MaxRetries = callback.MaxRetries ?? 0,
                        Headers = callback.Headers,
                        BatchId = batch.Id,
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Published the completion callback {TaskName} of batch {BatchId}",
                callback.TaskName,
                batch.Id
            );
        }
        catch (Exception)
        {
            // Nothing would dispatch the callback again if the claim stayed, so it is released
            // for the next completion of the batch
            await _batchStore
                .ReleaseCallbackClaimAsync(batch.Id, CancellationToken.None)
                .ConfigureAwait(false);

            throw;
        }
    }
}
