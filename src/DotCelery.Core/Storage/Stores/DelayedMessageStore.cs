using System.Runtime.CompilerServices;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IDelayedMessageStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// Due messages are claimed rather than removed, and each message is removed only when the
/// caller asks for the next one, that is, after it has handled the previous one. If the
/// caller crashes or stops early, the messages it did not finish are delivered again after
/// <see cref="StorageStoreOptions.ClaimTimeout"/>, so a message can be dispatched twice but
/// is never lost.
/// </remarks>
public sealed class DelayedMessageStore : IDelayedMessageStore
{
    private readonly string _queue;
    private const int ClaimBatchSize = 100;

    private readonly IQueueStore _queues;
    private readonly StorageStoreOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="DelayedMessageStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    public DelayedMessageStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _queues = storage.Queues;
        _options = options?.Value ?? new StorageStoreOptions();
        _queue = _options.Name("delayed");
    }

    /// <inheritdoc />
    public async ValueTask AddAsync(
        TaskMessage message,
        DateTimeOffset deliveryTime,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(message);

        await _queues
            .EnqueueAsync(
                _queue,
                message.Id,
                JsonSerializer.SerializeToUtf8Bytes(
                    message,
                    DotCeleryJsonContext.Default.TaskMessage
                ),
                deliveryTime,
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Due is judged by the storage provider's clock; <paramref name="now"/> is not used.
    /// </remarks>
    public async IAsyncEnumerable<TaskMessage> GetDueMessagesAsync(
        DateTimeOffset now,
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        while (true)
        {
            var items = await _queues
                .ClaimAsync(_queue, ClaimBatchSize, _options.ClaimTimeout, cancellationToken)
                .ConfigureAwait(false);

            if (items.Count == 0)
            {
                yield break;
            }

            foreach (var item in items)
            {
                yield return JsonSerializer.Deserialize(
                    item.Payload.Span,
                    DotCeleryJsonContext.Default.TaskMessage
                )!;

                // The caller has handled the message. If it re-added the message for a retry,
                // the claim is gone and the retry is kept.
                await _queues.CompleteAsync(item, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public ValueTask<bool> RemoveAsync(
        string taskId,
        CancellationToken cancellationToken = default
    ) => _queues.RemoveAsync(_queue, taskId, cancellationToken);

    /// <inheritdoc />
    public ValueTask<long> GetPendingCountAsync(CancellationToken cancellationToken = default) =>
        _queues.CountAsync(_queue, cancellationToken);

    /// <inheritdoc />
    public ValueTask<DateTimeOffset?> GetNextDeliveryTimeAsync(
        CancellationToken cancellationToken = default
    ) => _queues.GetNextDueAsync(_queue, cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
