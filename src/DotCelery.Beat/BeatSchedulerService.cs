using DotCelery.Client;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Beat;

/// <summary>
/// Background service that schedules periodic tasks.
/// </summary>
public sealed class BeatSchedulerService : BackgroundService
{
    private readonly Schedule _schedule;
    private readonly IMessageBroker _broker;
    private readonly IMessageSerializer _serializer;
    private readonly BeatOptions _options;
    private readonly ILogger<BeatSchedulerService> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="BeatSchedulerService"/> class.
    /// </summary>
    /// <param name="schedule">The schedule to execute.</param>
    /// <param name="broker">The message broker.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The scheduler options.</param>
    /// <param name="logger">The logger.</param>
    public BeatSchedulerService(
        Schedule schedule,
        IMessageBroker broker,
        IMessageSerializer serializer,
        IOptions<BeatOptions> options,
        ILogger<BeatSchedulerService> logger
    )
    {
        _schedule = schedule;
        _broker = broker;
        _serializer = serializer;
        _options = options.Value;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Beat scheduler starting with {Count} entries", _schedule.Count);

        if (_options.RunMissedOnStartup)
        {
            await RunMissedTasksAsync(stoppingToken).ConfigureAwait(false);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var now = DateTimeOffset.UtcNow;
                var dueEntries = _schedule.GetDueEntries(now).ToList();

                foreach (var entry in dueEntries)
                {
                    try
                    {
                        await ExecuteEntryAsync(entry, now, stoppingToken).ConfigureAwait(false);
                        entry.LastRunTime = now;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Failed to execute scheduled entry {EntryName}",
                            entry.Name
                        );
                    }
                }

                await Task.Delay(_options.CheckInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in beat scheduler loop");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);
            }
        }

        _logger.LogInformation("Beat scheduler stopped");
    }

    private async Task RunMissedTasksAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        foreach (var entry in _schedule)
        {
            if (entry.LastRunTime.HasValue)
            {
                var nextRun = entry.GetNextRunTime(entry.LastRunTime.Value);
                if (nextRun.HasValue && nextRun.Value < now)
                {
                    _logger.LogInformation("Running missed task {EntryName}", entry.Name);
                    try
                    {
                        await ExecuteEntryAsync(entry, now, cancellationToken)
                            .ConfigureAwait(false);
                        entry.LastRunTime = now;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "Failed to execute missed entry {EntryName}",
                            entry.Name
                        );
                    }
                }
            }
        }
    }

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
