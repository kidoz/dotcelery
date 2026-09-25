using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.Storage.Stores;

/// <summary>
/// Deletes expired entries from the registered <see cref="IStorageProvider"/> every
/// <see cref="StorageStoreOptions.PurgeInterval"/>.
/// </summary>
public sealed class StoragePurgeService : BackgroundService
{
    private readonly IStorageProvider _storage;
    private readonly StorageStoreOptions _options;
    private readonly ILogger<StoragePurgeService> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="StoragePurgeService"/> class.
    /// </summary>
    /// <param name="storage">The storage provider.</param>
    /// <param name="options">The store options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">The clock for the purge interval.</param>
    public StoragePurgeService(
        IStorageProvider storage,
        IOptions<StorageStoreOptions> options,
        ILogger<StoragePurgeService> logger,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(options);
        _storage = storage;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.PurgeInterval, _timeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await PurgeAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Stopping
        }
    }

    private async Task PurgeAsync(CancellationToken cancellationToken)
    {
        try
        {
            var purged = await _storage.PurgeExpiredAsync(cancellationToken).ConfigureAwait(false);

            if (purged > 0)
            {
                _logger.LogDebug(
                    "Deleted {Count} expired entries from {Storage} storage",
                    purged,
                    _storage.Name
                );
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The next purge deletes what this one could not
            _logger.LogWarning(
                ex,
                "Failed to delete expired entries from {Storage} storage",
                _storage.Name
            );
        }
    }
}
