using System.Runtime.CompilerServices;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.Serialization;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IRevocationStore"/> on the storage primitives.
/// </summary>
/// <remarks>
/// <para>
/// A revocation without <see cref="RevokeOptions.Expiry"/> is kept for
/// <see cref="StorageStoreOptions.RevocationRetention"/>.
/// </para>
/// <para>
/// Subscribers are woken by the provider's notifications when it has them, and otherwise
/// poll every <see cref="StorageStoreOptions.RevocationPollInterval"/>. Notifications are
/// best effort, so workers should also check <see cref="IsRevokedAsync"/> before running a task.
/// </para>
/// </remarks>
public sealed class RevocationStore : IRevocationStore
{
    private readonly string _collection;

    private static readonly TimeSpan PollLookback = TimeSpan.FromSeconds(30);

    private readonly IDocumentStore _documents;
    private readonly INotificationChannel? _notifications;
    private readonly StorageStoreOptions _options;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="RevocationStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="timeProvider">The clock for revocation times.</param>
    public RevocationStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _documents = storage.Documents;
        _notifications = storage.Notifications;
        _options = options?.Value ?? new StorageStoreOptions();
        _collection = _options.Name("revocations");
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        string taskId,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        options ??= RevokeOptions.Default;

        await _documents
            .UpsertAsync(
                _collection,
                taskId,
                options,
                DotCeleryJsonContext.Default.RevokeOptions,
                new DocumentWriteOptions
                {
                    TimeToLive = options.Expiry ?? _options.RevocationRetention,
                    SortKey = _timeProvider.GetUtcNow(),
                },
                cancellationToken
            )
            .ConfigureAwait(false);

        if (_notifications is not null)
        {
            await _notifications
                .PublishAsync(_collection, taskId, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask RevokeAsync(
        IEnumerable<string> taskIds,
        RevokeOptions? options = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(taskIds);

        foreach (var taskId in taskIds)
        {
            await RevokeAsync(taskId, options, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsRevokedAsync(
        string taskId,
        CancellationToken cancellationToken = default
    ) =>
        await _documents.GetAsync(_collection, taskId, cancellationToken).ConfigureAwait(false)
            is not null;

    /// <inheritdoc />
    public async IAsyncEnumerable<string> GetRevokedTaskIdsAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
        await foreach (
            var document in _documents
                .QueryAsync(_collection, DocumentFilter.All, cancellationToken: cancellationToken)
                .ConfigureAwait(false)
        )
        {
            yield return document.Id;
        }
    }

    /// <inheritdoc />
    public ValueTask<long> CleanupAsync(
        TimeSpan maxAge,
        CancellationToken cancellationToken = default
    ) =>
        _documents.DeleteManyAsync(
            _collection,
            new DocumentFilter { SortKeyBefore = _timeProvider.GetUtcNow() - maxAge },
            cancellationToken
        );

    /// <inheritdoc />
    public IAsyncEnumerable<RevocationEvent> SubscribeAsync(
        CancellationToken cancellationToken = default
    ) =>
        _notifications is not null
            ? ListenAsync(_notifications, cancellationToken)
            : PollAsync(cancellationToken);

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async IAsyncEnumerable<RevocationEvent> ListenAsync(
        INotificationChannel notifications,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await foreach (
            var taskId in notifications
                .SubscribeAsync(_collection, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            var document = await _documents
                .GetAsync(
                    _collection,
                    taskId,
                    DotCeleryJsonContext.Default.RevokeOptions,
                    cancellationToken
                )
                .ConfigureAwait(false);

            if (document is not null)
            {
                yield return ToEvent(document);
            }
        }
    }

    private async IAsyncEnumerable<RevocationEvent> PollAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var subscribedAt = _timeProvider.GetUtcNow();
        var seen = new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);

        while (true)
        {
            await Task.Delay(_options.RevocationPollInterval, _timeProvider, cancellationToken)
                .ConfigureAwait(false);

            // Revocations can commit out of time order, so each poll looks back and skips
            // the ones it has already returned
            var from = _timeProvider.GetUtcNow() - PollLookback;
            if (from < subscribedAt)
            {
                from = subscribedAt;
            }

            foreach (var (taskId, revokedAt) in seen.ToList())
            {
                if (revokedAt < from)
                {
                    seen.Remove(taskId);
                }
            }

            await foreach (
                var document in _documents
                    .QueryAsync(
                        _collection,
                        new DocumentFilter { SortKeyFrom = from },
                        DotCeleryJsonContext.Default.RevokeOptions,
                        cancellationToken: cancellationToken
                    )
                    .ConfigureAwait(false)
            )
            {
                var revokedAt = document.SortKey!.Value;
                if (!seen.TryGetValue(document.Id, out var seenAt) || seenAt != revokedAt)
                {
                    seen[document.Id] = revokedAt;
                    yield return ToEvent(document);
                }
            }
        }
    }

    private static RevocationEvent ToEvent(StoredDocument<RevokeOptions> document) =>
        new()
        {
            TaskId = document.Id,
            Options = document.Value,
            Timestamp = document.SortKey ?? DateTimeOffset.MinValue,
        };
}
