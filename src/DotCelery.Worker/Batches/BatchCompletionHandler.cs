using DotCelery.Core.Batches;
using DotCelery.Core.Signals;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Batches;

/// <summary>
/// Signal handler that updates batch state when tasks complete.
/// </summary>
public sealed class BatchCompletionHandler
    : ITaskSignalHandler<TaskSuccessSignal>,
        ITaskSignalHandler<TaskFailureSignal>,
        ITaskSignalHandler<TaskRevokedSignal>,
        ITaskSignalHandler<TaskRejectedSignal>
{
    private readonly IBatchStore _batchStore;
    private readonly ILogger<BatchCompletionHandler> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="BatchCompletionHandler"/> class.
    /// </summary>
    /// <param name="batchStore">The batch store.</param>
    /// <param name="logger">The logger.</param>
    public BatchCompletionHandler(IBatchStore batchStore, ILogger<BatchCompletionHandler> logger)
    {
        _batchStore = batchStore;
        _logger = logger;
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
            }
        }
    }
}
