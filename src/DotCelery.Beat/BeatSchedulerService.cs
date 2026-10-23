using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Beat;

/// <summary>
/// Background service that schedules periodic tasks.
/// </summary>
/// <remarks>
/// <para>
/// An entry runs one interval after it last ran, or at its next cron occurrence; a new entry
/// starts then too, rather than at startup. When the configured storage has a lease store, the
/// schedulers that share it elect one of themselves per
/// <see cref="BeatOptions.SchedulerName"/> and only that one runs the schedule; without storage
/// every scheduler runs it.
/// </para>
/// <para>
/// With <see cref="BeatOptions.PersistState"/> the last run times survive a restart, and
/// <see cref="BeatOptions.RunMissedOnStartup"/> decides whether runs missed while the scheduler
/// was stopped are run once at startup or skipped.
/// </para>
/// </remarks>
public sealed class BeatSchedulerService : BackgroundService
{
    private static readonly TimeSpan MinLeaderLease = TimeSpan.FromSeconds(30);

    private readonly Schedule _schedule;
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly BeatOptions _options;
    private readonly ILogger<BeatSchedulerService> _logger;
    private readonly TimeProvider _timeProvider;
    private readonly PartitionLockStore? _locks;
    private readonly string _schedulerId;
    private readonly string _leaderKey;
    private readonly string? _statePath;
    private readonly IDocumentStore? _sharedState;
    private readonly string _stateCollection;
    private bool _isLeader;
    private bool _missedRunsDone;

    /// <summary>
    /// Initializes a new instance of the <see cref="BeatSchedulerService"/> class.
    /// </summary>
    /// <param name="schedule">The schedule to execute.</param>
    /// <param name="broker">The message broker.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The scheduler options.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="timeProvider">The clock for scheduling.</param>
    /// <param name="storage">
    /// The storage provider schedulers elect a leader through, when one is registered.
    /// </param>
    /// <param name="storeOptions">The store options that name the leader's key.</param>
    public BeatSchedulerService(
        Schedule schedule,
        IMessageBroker broker,
        IMessageSerializer serializer,
        IOptions<BeatOptions> options,
        ILogger<BeatSchedulerService> logger,
        TimeProvider? timeProvider = null,
        IStorageProvider? storage = null,
        IOptions<StorageStoreOptions>? storeOptions = null
    )
    {
        ArgumentNullException.ThrowIfNull(schedule);
        ArgumentNullException.ThrowIfNull(broker);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);

        _schedule = schedule;
        _broker = broker;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _locks = storage is null
            ? null
            : new PartitionLockStore(
                storage,
                Options.Create(
                    new StorageStoreOptions
                    {
                        Prefix = storeOptions?.Value.Prefix ?? new StorageStoreOptions().Prefix,
                    }
                )
            );
        _sharedState = storage?.Documents;
        _stateCollection =
            (storeOptions?.Value ?? new StorageStoreOptions()).Prefix + ".beat-cursors";

        // Identifies this scheduler for the leader lease; two schedulers in one process must not
        // renew each other's lease
        _schedulerId =
            $"{_options.SchedulerName}-{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";
        _leaderKey = $"beat/{_options.SchedulerName}";
        _statePath = _options.PersistState
            ? _options.StatePath ?? $"{_options.SchedulerName}.state.json"
            : null;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Beat scheduler starting with {Count} entries", _schedule.Count);

        if (_locks is null)
        {
            _logger.LogWarning(
                "The Beat scheduler has no storage provider, so every scheduler runs the "
                    + "schedule; register a storage provider to elect a single scheduler"
            );
        }

        if (_sharedState is not null)
        {
            await ExecuteSharedAsync(stoppingToken).ConfigureAwait(false);
            return;
        }

        var now = _timeProvider.GetUtcNow();
        await LoadStateAsync(stoppingToken).ConfigureAwait(false);
        AnchorEntries(now);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (await IsLeaderAsync(stoppingToken).ConfigureAwait(false))
                    {
                        if (_options.RunMissedOnStartup && !_missedRunsDone)
                        {
                            await RunMissedEntriesAsync(stoppingToken).ConfigureAwait(false);
                            _missedRunsDone = true;
                            await SaveStateAsync(stoppingToken).ConfigureAwait(false);
                        }

                        await RunDueEntriesAsync(stoppingToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in beat scheduler loop");
                    await DelayAsync(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);
                    continue;
                }

                await DelayAsync(_options.CheckInterval, stoppingToken).ConfigureAwait(false);
            }
        }
        finally
        {
            await ReleaseLeadershipAsync().ConfigureAwait(false);
        }

        _logger.LogInformation("Beat scheduler stopped");
    }

    private async Task ExecuteSharedAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (await IsLeaderAsync(stoppingToken).ConfigureAwait(false))
                    {
                        using var tick = CancellationTokenSource.CreateLinkedTokenSource(
                            stoppingToken
                        );
                        var renewal = RenewLeadershipAsync(tick);
                        try
                        {
                            foreach (var entry in _schedule)
                                await PublishSharedEntryAsync(entry, tick.Token)
                                    .ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (tick.IsCancellationRequested) { }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(
                                ex,
                                "Shared Beat tick failed; pending occurrences remain recoverable"
                            );
                        }
                        finally
                        {
                            await tick.CancelAsync().ConfigureAwait(false);
                            try
                            {
                                await renewal.ConfigureAwait(false);
                            }
                            catch (OperationCanceledException) when (tick.IsCancellationRequested)
                            { }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(
                                    ex,
                                    "Beat lost its renewal; the next leader resumes shared pending occurrences"
                                );
                            }
                        }
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Shared Beat could not acquire leadership; retrying on the next check"
                    );
                }
                await DelayAsync(_options.CheckInterval, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            await ReleaseLeadershipAsync().ConfigureAwait(false);
        }
    }

    private async Task RenewLeadershipAsync(CancellationTokenSource tick)
    {
        var duration = _options.CheckInterval * 3;
        if (duration < MinLeaderLease)
            duration = MinLeaderLease;
        try
        {
            while (true)
            {
                await Task.Delay(duration / 3, _timeProvider, tick.Token).ConfigureAwait(false);
                if (!await IsLeaderAsync(tick.Token).ConfigureAwait(false))
                {
                    await tick.CancelAsync().ConfigureAwait(false);
                    return;
                }
            }
        }
        catch (Exception) when (!tick.IsCancellationRequested)
        {
            await tick.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task PublishSharedEntryAsync(
        ScheduleEntry entry,
        CancellationToken cancellationToken
    )
    {
        if (!entry.Enabled)
            return;
        var documents =
            _sharedState ?? throw new InvalidOperationException("Shared storage is unavailable.");
        var key = $"{_options.SchedulerName.Length}:{_options.SchedulerName}:{entry.Name}";
        var now = _timeProvider.GetUtcNow();
        var info = SharedBeatJsonContext.Default.BeatEntryState;
        var current = await documents
            .GetAsync(_stateCollection, key, info, cancellationToken)
            .ConfigureAwait(false);
        if (current is null)
        {
            var anchor = new BeatEntryState(entry.LastRunTime ?? now, null);
            if (!_options.RunMissedOnStartup && entry.GetNextRunTime(anchor.LastRunTime) < now)
                anchor = anchor with { LastRunTime = now };
            await documents
                .TryInsertAsync(
                    _stateCollection,
                    key,
                    anchor,
                    info,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            current = await documents
                .GetAsync(_stateCollection, key, info, cancellationToken)
                .ConfigureAwait(false);
        }
        if (current is null)
            return;
        entry.LastRunTime = current.Value.LastRunTime;
        var pending = current.Value.PendingPayload is { Length: > 0 } payload
            ? JsonSerializer.Deserialize(payload, DotCeleryJsonContext.Default.TaskMessage)
            : null;
        var version = current.Version;
        if (pending is null)
        {
            if (entry.GetNextRunTime(current.Value.LastRunTime) is not { } due || due > now)
                return;
            pending = CreateEntryMessage(entry, now);
            var claimed = current.Value with
            {
                PendingPayload = JsonSerializer.SerializeToUtf8Bytes(
                    pending,
                    DotCeleryJsonContext.Default.TaskMessage
                ),
            };
            var replaced = await documents
                .TryReplaceAsync(
                    _stateCollection,
                    key,
                    claimed,
                    version,
                    info,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false);
            if (replaced is null)
                return;
            version = replaced.Value;
        }
        cancellationToken.ThrowIfCancellationRequested();
        if (!await IsLeaderAsync(cancellationToken).ConfigureAwait(false))
            return;
        using var tenant = TenantContext.SetTenant(pending.TenantId);
        await _broker.PublishAsync(pending, cancellationToken).ConfigureAwait(false);
        var completed = new BeatEntryState(now, null);
        if (
            await documents
                .TryReplaceAsync(
                    _stateCollection,
                    key,
                    completed,
                    version,
                    info,
                    cancellationToken: cancellationToken
                )
                .ConfigureAwait(false)
            is not null
        )
            entry.LastRunTime = now;
    }

    // Entries without a remembered run start one interval or cron occurrence from now, and
    // entries whose runs were missed are re-anchored when those runs are not run at startup
    private void AnchorEntries(DateTimeOffset now)
    {
        foreach (var entry in _schedule)
        {
            if (entry.LastRunTime is not { } lastRun)
            {
                entry.LastRunTime = now;
                continue;
            }

            if (
                !_options.RunMissedOnStartup
                && entry.GetNextRunTime(lastRun) is { } nextRun
                && nextRun < now
            )
            {
                _logger.LogDebug(
                    "Skipping the runs of {EntryName} missed while stopped",
                    entry.Name
                );
                entry.LastRunTime = now;
            }
        }
    }

    private async Task RunDueEntriesAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();

        foreach (var entry in _schedule.GetDueEntries(now).ToList())
        {
            try
            {
                await ExecuteEntryAsync(entry, now, cancellationToken).ConfigureAwait(false);
                entry.LastRunTime = now;
                await SaveStateAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                // The entry stays due and is published again on the next check
                _logger.LogError(ex, "Failed to execute scheduled entry {EntryName}", entry.Name);
            }
        }
    }

    private async Task RunMissedEntriesAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();

        foreach (var entry in _schedule)
        {
            if (
                entry.LastRunTime is not { } lastRun
                || entry.GetNextRunTime(lastRun) is not { } nextRun
                || nextRun >= now
            )
            {
                continue;
            }

            _logger.LogInformation("Running missed task {EntryName}", entry.Name);

            try
            {
                await ExecuteEntryAsync(entry, now, cancellationToken).ConfigureAwait(false);
                entry.LastRunTime = now;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to execute missed entry {EntryName}", entry.Name);
            }
        }
    }

    // Only the scheduler that holds the leader lease runs the schedule; the others take over
    // when it stops or its lease expires
    private async ValueTask<bool> IsLeaderAsync(CancellationToken cancellationToken)
    {
        if (_locks is null)
        {
            return true;
        }

        var lease = _options.CheckInterval * 3;
        var acquired = await _locks
            .TryAcquireAsync(
                _leaderKey,
                _schedulerId,
                lease < MinLeaderLease ? MinLeaderLease : lease,
                cancellationToken
            )
            .ConfigureAwait(false);

        if (_isLeader && !acquired)
        {
            _logger.LogWarning("Another Beat scheduler took the schedule over");
        }

        _isLeader = acquired;
        return acquired;
    }

    private async ValueTask ReleaseLeadershipAsync()
    {
        if (_locks is null || !_isLeader)
        {
            return;
        }

        try
        {
            await _locks
                .ReleaseAsync(_leaderKey, _schedulerId, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to release the Beat leader lease");
        }

        _isLeader = false;
    }

    private async ValueTask LoadStateAsync(CancellationToken cancellationToken)
    {
        if (_statePath is null)
        {
            return;
        }

        var state = await ScheduleStateStore
            .LoadAsync(_statePath, cancellationToken)
            .ConfigureAwait(false);

        if (state.Count == 0)
        {
            return;
        }

        foreach (var entry in _schedule)
        {
            if (state.TryGetValue(entry.Name, out var lastRun))
            {
                entry.LastRunTime = lastRun;
            }
        }

        _logger.LogInformation("Loaded the last run time of {Count} schedule entries", state.Count);
    }

    private async ValueTask SaveStateAsync(CancellationToken cancellationToken)
    {
        if (_statePath is null)
        {
            return;
        }

        try
        {
            await ScheduleStateStore
                .SaveAsync(_statePath, _schedule, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            _logger.LogWarning(ex, "Failed to persist the Beat schedule state");
        }
    }

    private Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken) =>
        Task.Delay(delay, _timeProvider, cancellationToken);

    private TaskMessage CreateEntryMessage(ScheduleEntry entry, DateTimeOffset now)
    {
        var taskId = Guid.NewGuid().ToString("N");
        var signature = entry.Task;

        // Apply jitter if configured
        var eta = now;
        if (_options.MaxJitter > TimeSpan.Zero)
        {
            var jitterMs = Random.Shared.Next(0, (int)_options.MaxJitter.TotalMilliseconds);
            eta = now.AddMilliseconds(jitterMs);
        }

        var message = TaskMessageFactory.ForTask(
            taskId,
            signature.TaskName,
            signature.Args ?? [],
            _serializer.ContentType,
            now,
            entry.Options?.Queue ?? signature.Queue,
            eta: eta > now ? eta : null,
            expires: entry.Options?.ExpiresIn.HasValue == true
                ? now + entry.Options.ExpiresIn.Value
                : signature.Expires,
            maxRetries: signature.MaxRetries,
            priority: entry.Options?.Priority ?? signature.Priority,
            tenantId: signature.Headers.GetTenantId(),
            headers: signature.Headers
        );

        return message;
    }

    private async Task ExecuteEntryAsync(
        ScheduleEntry entry,
        DateTimeOffset now,
        CancellationToken cancellationToken
    )
    {
        var message = CreateEntryMessage(entry, now);
        await _broker.PublishAsync(message, cancellationToken).ConfigureAwait(false);
    }
}
