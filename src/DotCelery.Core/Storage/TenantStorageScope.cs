using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage;

/// <summary>Applies an authenticated ambient tenant to every storage primitive operation.</summary>
public static class TenantStorageScope
{
    /// <summary>Wraps primitives when tenant isolation is required; transaction ownership stays with the original provider.</summary>
    /// <param name="storage">The underlying provider.</param>
    /// <param name="options">The store isolation options.</param>
    /// <returns>Primitives that fail closed without a tenant scope.</returns>
    public static IStorageProvider For(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        return options?.Value.RequireTenantScope == true ? new ScopedProvider(storage) : storage;
    }

    private static string Prefix =>
        "t."
        + Convert.ToHexStringLower(
            SHA256.HashData(
                Encoding.UTF8.GetBytes(
                    TenantContext.Current.TenantId is { Length: > 0 } tenant
                        ? tenant
                        : throw new InvalidOperationException(
                            "An authenticated tenant scope is required for storage access."
                        )
                )
            )
        )
        + ".";

    private static string Scope(string name)
    {
        StorageGuard.ThrowIfInvalidName(name);
        return Prefix
            + Convert
                .ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(name)))
                .TrimEnd('=')
                .Replace('+', '-')
                .Replace('/', '_');
    }

    private static string ScopeKey(string key)
    {
        StorageGuard.ThrowIfInvalidKey(key);
        return Prefix + key;
    }

    private sealed class ScopedProvider(IStorageProvider inner) : IStorageProvider
    {
        public string Name => inner.Name;
        public IDocumentStore Documents { get; } = new Documents(inner.Documents);
        public IQueueStore Queues { get; } = new Queues(inner.Queues);
        public ILeaseStore Leases { get; } = new Leases(inner.Leases);
        public ICounterStore Counters { get; } = new Counters(inner.Counters);
        public INotificationChannel? Notifications { get; } =
            inner.Notifications is { } channel ? new Notifications(channel) : null;

        public ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default) =>
            inner.PurgeExpiredAsync(cancellationToken);

        public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
            inner.IsHealthyAsync(cancellationToken);
    }

    private sealed class Documents(IDocumentStore inner) : IDocumentStore
    {
        public ValueTask<StoredDocument?> GetAsync(
            string collection,
            string id,
            CancellationToken cancellationToken = default
        ) => inner.GetAsync(Scope(collection), id, cancellationToken);

        public ValueTask<long?> TryInsertAsync(
            string collection,
            string id,
            ReadOnlyMemory<byte> value,
            DocumentWriteOptions? options = null,
            CancellationToken cancellationToken = default
        ) => inner.TryInsertAsync(Scope(collection), id, value, options, cancellationToken);

        public ValueTask<long?> TryReplaceAsync(
            string collection,
            string id,
            ReadOnlyMemory<byte> value,
            long expectedVersion,
            DocumentWriteOptions? options = null,
            CancellationToken cancellationToken = default
        ) =>
            inner.TryReplaceAsync(
                Scope(collection),
                id,
                value,
                expectedVersion,
                options,
                cancellationToken
            );

        public ValueTask<long> UpsertAsync(
            string collection,
            string id,
            ReadOnlyMemory<byte> value,
            DocumentWriteOptions? options = null,
            CancellationToken cancellationToken = default
        ) => inner.UpsertAsync(Scope(collection), id, value, options, cancellationToken);

        public ValueTask<bool> DeleteAsync(
            string collection,
            string id,
            long? expectedVersion = null,
            CancellationToken cancellationToken = default
        ) => inner.DeleteAsync(Scope(collection), id, expectedVersion, cancellationToken);

        public IAsyncEnumerable<StoredDocument> QueryAsync(
            string collection,
            DocumentFilter filter,
            DocumentPage? page = null,
            CancellationToken cancellationToken = default
        ) => inner.QueryAsync(Scope(collection), filter, page, cancellationToken);

        public ValueTask<long> CountAsync(
            string collection,
            DocumentFilter filter,
            CancellationToken cancellationToken = default
        ) => inner.CountAsync(Scope(collection), filter, cancellationToken);

        public ValueTask<long> DeleteManyAsync(
            string collection,
            DocumentFilter filter,
            CancellationToken cancellationToken = default
        ) => inner.DeleteManyAsync(Scope(collection), filter, cancellationToken);
    }

    private sealed class Queues(IQueueStore inner) : IQueueStore
    {
        public ValueTask EnqueueAsync(
            string queue,
            string id,
            ReadOnlyMemory<byte> payload,
            DateTimeOffset dueAt,
            CancellationToken cancellationToken = default
        ) => inner.EnqueueAsync(Scope(queue), id, payload, dueAt, cancellationToken);

        public async ValueTask<IReadOnlyList<QueueItem>> ClaimAsync(
            string queue,
            int maxItems,
            TimeSpan visibilityTimeout,
            CancellationToken cancellationToken = default
        ) =>
            (
                await inner
                    .ClaimAsync(Scope(queue), maxItems, visibilityTimeout, cancellationToken)
                    .ConfigureAwait(false)
            )
                .Select(item => item with { Queue = queue })
                .ToArray();

        public ValueTask<bool> CompleteAsync(
            QueueItem item,
            CancellationToken cancellationToken = default
        ) => inner.CompleteAsync(item with { Queue = Scope(item.Queue) }, cancellationToken);

        public ValueTask<bool> AbandonAsync(
            QueueItem item,
            DateTimeOffset? dueAt = null,
            CancellationToken cancellationToken = default
        ) => inner.AbandonAsync(item with { Queue = Scope(item.Queue) }, dueAt, cancellationToken);

        public ValueTask<bool> RemoveAsync(
            string queue,
            string id,
            CancellationToken cancellationToken = default
        ) => inner.RemoveAsync(Scope(queue), id, cancellationToken);

        public ValueTask<long> CountAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) => inner.CountAsync(Scope(queue), cancellationToken);

        public ValueTask<DateTimeOffset?> GetNextDueAsync(
            string queue,
            CancellationToken cancellationToken = default
        ) => inner.GetNextDueAsync(Scope(queue), cancellationToken);
    }

    private sealed class Leases(ILeaseStore inner) : ILeaseStore
    {
        public async ValueTask<Lease?> TryAcquireAsync(
            string key,
            string owner,
            TimeSpan duration,
            CancellationToken cancellationToken = default
        ) =>
            (
                await inner
                    .TryAcquireAsync(ScopeKey(key), owner, duration, cancellationToken)
                    .ConfigureAwait(false)
            )
                is { } lease
                ? lease with
                {
                    Key = key,
                }
                : null;

        public async ValueTask<Lease?> RenewAsync(
            Lease lease,
            TimeSpan duration,
            CancellationToken cancellationToken = default
        ) =>
            (
                await inner
                    .RenewAsync(
                        lease with
                        {
                            Key = ScopeKey(lease.Key),
                        },
                        duration,
                        cancellationToken
                    )
                    .ConfigureAwait(false)
            )
                is { } renewed
                ? renewed with
                {
                    Key = lease.Key,
                }
                : null;

        public ValueTask<bool> ReleaseAsync(
            Lease lease,
            CancellationToken cancellationToken = default
        ) => inner.ReleaseAsync(lease with { Key = ScopeKey(lease.Key) }, cancellationToken);

        public async ValueTask<Lease?> GetAsync(
            string key,
            CancellationToken cancellationToken = default
        ) =>
            (await inner.GetAsync(ScopeKey(key), cancellationToken).ConfigureAwait(false))
                is { } lease
                ? lease with
                {
                    Key = key,
                }
                : null;

        public async IAsyncEnumerable<Lease> ListAsync(
            string keyPrefix,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            var prefix = Prefix;
            await foreach (
                var lease in inner
                    .ListAsync(prefix + keyPrefix, cancellationToken)
                    .ConfigureAwait(false)
            )
                yield return lease with
                {
                    Key = lease.Key[prefix.Length..],
                };
        }
    }

    private sealed class Counters(ICounterStore inner) : ICounterStore
    {
        public ValueTask<long> IncrementAsync(
            string key,
            long delta = 1,
            TimeSpan? timeToLive = null,
            CancellationToken cancellationToken = default
        ) => inner.IncrementAsync(ScopeKey(key), delta, timeToLive, cancellationToken);

        public ValueTask<long> GetAsync(
            string key,
            CancellationToken cancellationToken = default
        ) => inner.GetAsync(ScopeKey(key), cancellationToken);

        public ValueTask<bool> DeleteAsync(
            string key,
            CancellationToken cancellationToken = default
        ) => inner.DeleteAsync(ScopeKey(key), cancellationToken);

        public ValueTask<WindowResult> TryAddToWindowAsync(
            string key,
            int limit,
            TimeSpan window,
            CancellationToken cancellationToken = default
        ) => inner.TryAddToWindowAsync(ScopeKey(key), limit, window, cancellationToken);

        public ValueTask<WindowSnapshot> GetWindowAsync(
            string key,
            TimeSpan window,
            CancellationToken cancellationToken = default
        ) => inner.GetWindowAsync(ScopeKey(key), window, cancellationToken);
    }

    private sealed class Notifications(INotificationChannel inner) : INotificationChannel
    {
        public ValueTask PublishAsync(
            string channel,
            string message,
            CancellationToken cancellationToken = default
        ) => inner.PublishAsync(Scope(channel), message, cancellationToken);

        public IAsyncEnumerable<string> SubscribeAsync(
            string channel,
            CancellationToken cancellationToken = default
        ) => inner.SubscribeAsync(Scope(channel), cancellationToken);
    }
}
