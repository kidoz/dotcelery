using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Client.Batches;

/// <summary>
/// Default implementation of <see cref="IBatchClient"/>.
/// </summary>
public sealed class BatchClient : IBatchClient
{
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly IBatchStore? _batchStore;
    private readonly ICeleryClient _celeryClient;
    private readonly CeleryClientOptions _options;
    private readonly ILogger<BatchClient> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="BatchClient"/> class.
    /// </summary>
    public BatchClient(
        IMessageBroker broker,
        IMessageSerializer serializer,
        ICeleryClient celeryClient,
        IOptions<CeleryClientOptions> options,
        ILogger<BatchClient> logger,
        IBatchStore? batchStore = null
    )
    {
        _broker = broker;
        _serializer = serializer;
        _celeryClient = celeryClient;
        _batchStore = batchStore;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    public async ValueTask<string> CreateBatchAsync(
        Action<BatchBuilder> configure,
        CancellationToken cancellationToken = default
    )
    {
        var builder = new BatchBuilder();
        configure(builder);

        if (builder.Tasks.Count == 0)
        {
            throw new ArgumentException("Batch must contain at least one task.");
        }

        var batchId = Guid.NewGuid().ToString("N");
        var taskIds = new List<string>();

        // Publish all tasks in the batch
        foreach (var batchTask in builder.Tasks)
        {
            var taskId = batchTask.TaskId ?? Guid.NewGuid().ToString("N");
            taskIds.Add(taskId);

            // Serialize the input
            var args = batchTask.Input is not null ? _serializer.Serialize(batchTask.Input) : [];

            var message = new TaskMessage
            {
                Id = taskId,
                Task = batchTask.TaskName,
                Args = args,
                ContentType = _serializer.ContentType,
                Timestamp = DateTimeOffset.UtcNow,
                Queue = batchTask.Queue ?? _options.DefaultQueue,
                MaxRetries = batchTask.MaxRetries ?? _options.DefaultMaxRetries,
                Priority = batchTask.Priority ?? 0,
                Headers = batchTask.Headers,
                BatchId = batchId,
            };

            await _broker.PublishAsync(message, cancellationToken).ConfigureAwait(false);
        }

        // Create batch record if store is available
        if (_batchStore is not null)
        {
            var batch = new Batch
            {
                Id = batchId,
                Name = builder.Name,
                State = BatchState.Pending,
                TaskIds = taskIds,
                CreatedAt = DateTimeOffset.UtcNow,
                CallbackTaskId = builder.Callback?.TaskId,
            };

            await _batchStore.CreateAsync(batch, cancellationToken).ConfigureAwait(false);
        }

        _logger.LogInformation(
            "Created batch {BatchId} with {TaskCount} tasks",
            batchId,
            taskIds.Count
        );

        return batchId;
    }

    /// <inheritdoc />
    public async ValueTask<Batch?> GetBatchAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        if (_batchStore is null)
        {
            throw new InvalidOperationException(
                "Batch operations require an IBatchStore implementation to be registered."
            );
        }

        return await _batchStore.GetAsync(batchId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<Batch> WaitForBatchAsync(
        string batchId,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        if (_batchStore is null)
        {
            throw new InvalidOperationException(
                "Batch operations require an IBatchStore implementation to be registered."
            );
        }

        timeout ??= TimeSpan.FromMinutes(30);
        pollInterval ??= TimeSpan.FromSeconds(1);

        using var timeoutCts = new CancellationTokenSource(timeout.Value);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutCts.Token
        );

        while (!linkedCts.Token.IsCancellationRequested)
        {
            var batch = await _batchStore.GetAsync(batchId, linkedCts.Token).ConfigureAwait(false);
            if (batch is null)
            {
                throw new InvalidOperationException($"Batch {batchId} not found.");
            }

            if (batch.IsFinished)
            {
                return batch;
            }

            await Task.Delay(pollInterval.Value, linkedCts.Token).ConfigureAwait(false);
        }

        throw new TimeoutException($"Batch {batchId} did not complete within {timeout.Value}.");
    }

    /// <inheritdoc />
    public async ValueTask CancelBatchAsync(
        string batchId,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(batchId);

        if (_batchStore is null)
        {
            throw new InvalidOperationException(
                "Batch operations require an IBatchStore implementation to be registered."
            );
        }

        var batch = await _batchStore.GetAsync(batchId, cancellationToken).ConfigureAwait(false);
        if (batch is null)
        {
            throw new InvalidOperationException($"Batch {batchId} not found.");
        }

        // Revoke all pending tasks
        var pendingTaskIds = batch
            .TaskIds.Except(batch.CompletedTaskIds)
            .Except(batch.FailedTaskIds)
            .ToList();

        if (pendingTaskIds.Count > 0)
        {
            await _celeryClient
                .RevokeAsync(pendingTaskIds, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        // Update batch state
        await _batchStore
            .UpdateStateAsync(batchId, BatchState.Cancelled, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Cancelled batch {BatchId}, revoked {RevokedCount} pending tasks",
            batchId,
            pendingTaskIds.Count
        );
    }
}
