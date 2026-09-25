using DotCelery.Core.Execution;
using DotCelery.Core.Partitioning;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// <see cref="IPartitionLockStore"/> on the storage primitives. Each lock is a lease held by
/// the task ID, so only the holder can extend or release it.
/// </summary>
public sealed class PartitionLockStore : IPartitionLockStore
{
    private readonly ILeaseStore _leases;
    private readonly string _keyPrefix;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionLockStore"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    public PartitionLockStore(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _leases = storage.Leases;
        _keyPrefix = (options?.Value ?? new StorageStoreOptions()).Name("partition/");
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(
        string partitionKey,
        string taskId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default
    ) =>
        await _leases
            .TryAcquireAsync(_keyPrefix + partitionKey, taskId, timeout, cancellationToken)
            .ConfigureAwait(false)
            is not null;

    /// <inheritdoc />
    public async ValueTask<bool> ReleaseAsync(
        string partitionKey,
        string taskId,
        CancellationToken cancellationToken = default
    )
    {
        var lease = await _leases
            .GetAsync(_keyPrefix + partitionKey, cancellationToken)
            .ConfigureAwait(false);

        // Releasing by token cannot affect a holder that took over in the meantime
        return lease?.Owner == taskId
            && await _leases.ReleaseAsync(lease, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsLockedAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    ) =>
        await _leases.GetAsync(_keyPrefix + partitionKey, cancellationToken).ConfigureAwait(false)
            is not null;

    /// <inheritdoc />
    public async ValueTask<string?> GetLockHolderAsync(
        string partitionKey,
        CancellationToken cancellationToken = default
    ) =>
        (
            await _leases
                .GetAsync(_keyPrefix + partitionKey, cancellationToken)
                .ConfigureAwait(false)
        )?.Owner;

    /// <inheritdoc />
    /// <remarks>The lock then expires <paramref name="extension"/> from now.</remarks>
    public async ValueTask<bool> ExtendAsync(
        string partitionKey,
        string taskId,
        TimeSpan extension,
        CancellationToken cancellationToken = default
    )
    {
        var lease = await _leases
            .GetAsync(_keyPrefix + partitionKey, cancellationToken)
            .ConfigureAwait(false);

        return lease?.Owner == taskId
            && await _leases.RenewAsync(lease, extension, cancellationToken).ConfigureAwait(false)
                is not null;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// <see cref="ITaskExecutionTracker"/> on the storage primitives. Each execution is a lease
/// on the task name and key, held by the task ID.
/// </summary>
public sealed class TaskExecutionTracker : ITaskExecutionTracker
{
    private readonly ILeaseStore _leases;
    private readonly StorageStoreOptions _options;
    private readonly string _keyPrefix;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutionTracker"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    public TaskExecutionTracker(
        IStorageProvider storage,
        IOptions<StorageStoreOptions>? options = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        _leases = storage.Leases;
        _options = options?.Value ?? new StorageStoreOptions();
        _keyPrefix = _options.Name("execution/");
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryStartAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    ) =>
        await _leases
            .TryAcquireAsync(
                LeaseKey(taskName, key),
                taskId,
                timeout ?? _options.ExecutionTimeout,
                cancellationToken
            )
            .ConfigureAwait(false)
            is not null;

    /// <inheritdoc />
    public async ValueTask StopAsync(
        string taskName,
        string taskId,
        string? key = null,
        CancellationToken cancellationToken = default
    )
    {
        var lease = await _leases
            .GetAsync(LeaseKey(taskName, key), cancellationToken)
            .ConfigureAwait(false);

        if (lease?.Owner == taskId)
        {
            await _leases.ReleaseAsync(lease, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsExecutingAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    ) =>
        await _leases.GetAsync(LeaseKey(taskName, key), cancellationToken).ConfigureAwait(false)
            is not null;

    /// <inheritdoc />
    public async ValueTask<string?> GetExecutingTaskIdAsync(
        string taskName,
        string? key = null,
        CancellationToken cancellationToken = default
    ) =>
        (
            await _leases.GetAsync(LeaseKey(taskName, key), cancellationToken).ConfigureAwait(false)
        )?.Owner;

    /// <inheritdoc />
    /// <remarks>The execution then expires <paramref name="extension"/> from now.</remarks>
    public async ValueTask<bool> ExtendAsync(
        string taskName,
        string taskId,
        string? key = null,
        TimeSpan? extension = null,
        CancellationToken cancellationToken = default
    )
    {
        var lease = await _leases
            .GetAsync(LeaseKey(taskName, key), cancellationToken)
            .ConfigureAwait(false);

        return lease?.Owner == taskId
            && await _leases
                .RenewAsync(lease, extension ?? _options.ExecutionTimeout, cancellationToken)
                .ConfigureAwait(false)
                is not null;
    }

    /// <inheritdoc />
    /// <remarks>Entries are keyed by task name, or by <c>{taskName}:{key}</c> when a key is used.</remarks>
    public async ValueTask<IReadOnlyDictionary<string, ExecutingTaskInfo>> GetAllExecutingAsync(
        CancellationToken cancellationToken = default
    )
    {
        var executing = new Dictionary<string, ExecutingTaskInfo>(StringComparer.Ordinal);

        await foreach (
            var lease in _leases.ListAsync(_keyPrefix, cancellationToken).ConfigureAwait(false)
        )
        {
            var parts = lease.Key[_keyPrefix.Length..].Split('/');
            var taskName = Uri.UnescapeDataString(parts[0]);
            var key = parts.Length > 1 ? Uri.UnescapeDataString(parts[1]) : null;

            executing[key is null ? taskName : $"{taskName}:{key}"] = new ExecutingTaskInfo
            {
                TaskId = lease.Owner,
                Key = key,
                StartedAt = lease.AcquiredAt,
                ExpiresAt = lease.ExpiresAt,
            };
        }

        return executing;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // Escaping keeps task names and keys that contain '/' unambiguous
    private string LeaseKey(string taskName, string? key)
    {
        ArgumentException.ThrowIfNullOrEmpty(taskName);

        return key is null
            ? _keyPrefix + Uri.EscapeDataString(taskName)
            : $"{_keyPrefix}{Uri.EscapeDataString(taskName)}/{Uri.EscapeDataString(key)}";
    }
}
