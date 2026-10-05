using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
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
        _locks = storage is null ? null : new PartitionLockStore(storage, storeOptions);

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

    private async Task ExecuteEntryAsync(
        ScheduleEntry entry,
        DateTimeOffset now,
        CancellationToken cancellationToken
    )
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

        var message = new TaskMessage
        {
            Id = taskId,
            Task = signature.TaskName,
            Args = signature.Args ?? [],
            ContentType = _serializer.ContentType,
            Timestamp = now,
            Eta = eta > now ? eta : null,
            Expires =
                entry.Options?.ExpiresIn.HasValue == true
                    ? now + entry.Options.ExpiresIn.Value
                    : signature.Expires,
            MaxRetries = signature.MaxRetries,
            Priority = entry.Options?.Priority ?? signature.Priority,
            Queue = entry.Options?.Queue ?? signature.Queue,
            Headers = signature.Headers,
        };

        await _broker.PublishAsync(message, cancellationToken).ConfigureAwait(false);

        _logger.LogDebug(
            "Scheduled task {TaskName} (ID: {TaskId}) from entry {EntryName}",
            signature.TaskName,
            taskId,
            entry.Name
        );
    }
}
