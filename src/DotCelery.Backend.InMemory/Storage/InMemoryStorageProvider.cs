using DotCelery.Core.Storage;

namespace DotCelery.Backend.InMemory.Storage;

/// <summary>
/// Storage primitives kept in process memory, for tests and local development. Data is lost
/// when the process exits and is not shared between processes.
/// </summary>
public sealed class InMemoryStorageProvider : IStorageProvider
{
    private readonly InMemoryDocumentStore _documents;
    private readonly InMemoryLeaseStore _leases;
    private readonly InMemoryCounterStore _counters;

    /// <summary>
    /// Initializes a new instance of the <see cref="InMemoryStorageProvider"/> class.
    /// </summary>
    /// <param name="timeProvider">The clock for expiry, due times, and leases.</param>
    public InMemoryStorageProvider(TimeProvider? timeProvider = null)
    {
        var time = timeProvider ?? TimeProvider.System;

        _documents = new InMemoryDocumentStore(time);
        _leases = new InMemoryLeaseStore(time);
        _counters = new InMemoryCounterStore(time);
        Queues = new InMemoryQueueStore(time);
        Notifications = new InMemoryNotificationChannel();
    }

    /// <inheritdoc />
    public string Name => "InMemory";

    /// <inheritdoc />
    public IDocumentStore Documents => _documents;

    /// <inheritdoc />
    public ILeaseStore Leases => _leases;

    /// <inheritdoc />
    public IQueueStore Queues { get; }

    /// <inheritdoc />
    public ICounterStore Counters => _counters;

    /// <inheritdoc />
    public INotificationChannel? Notifications { get; }

    /// <inheritdoc />
    public ValueTask<long> PurgeExpiredAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(
            _documents.PurgeExpired() + _leases.PurgeExpired() + _counters.PurgeExpired()
        );

    /// <inheritdoc />
    public ValueTask<bool> IsHealthyAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(true);
}
