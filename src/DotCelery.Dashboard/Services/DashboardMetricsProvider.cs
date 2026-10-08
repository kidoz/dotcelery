using DotCelery.Core.Abstractions;
using DotCelery.Core.Dashboard;
using DotCelery.Core.Models;

namespace DotCelery.Dashboard.Services;

/// <summary>
/// Serves the dashboard's metrics: the configured window aggregated from the historical data
/// store when one is registered, and otherwise computed live from the task records.
/// </summary>
public sealed class DashboardMetricsProvider : IMetricsProvider
{
    private const int WindowScanLimit = 10_000;

    private readonly TaskRecordStore _records;
    private readonly IHistoricalDataStore? _history;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardMetricsProvider"/> class.
    /// </summary>
    /// <param name="records">The task records, for the live window.</param>
    /// <param name="history">The historical data store, when one is registered.</param>
    /// <param name="timeProvider">The clock for the window.</param>
    public DashboardMetricsProvider(
        TaskRecordStore records,
        IHistoricalDataStore? history = null,
        TimeProvider? timeProvider = null
    )
    {
        _records = records;
        _history = history;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async ValueTask<DashboardMetrics> GetMetricsAsync(
        TimeSpan window,
        CancellationToken cancellationToken = default
    )
    {
        var now = _timeProvider.GetUtcNow();
        var from = now - window;

        if (_history is not null)
        {
            var aggregated = await _history
                .GetMetricsAsync(from, now, MetricsGranularity.Minute, cancellationToken)
                .ConfigureAwait(false);
            var byTaskName = await _history
                .GetMetricsByTaskNameAsync(from, now, cancellationToken)
                .ConfigureAwait(false);

            return new DashboardMetrics
            {
                Window = window,
                TotalProcessed = aggregated.TotalProcessed,
                SuccessCount = aggregated.SuccessCount,
                FailureCount = aggregated.FailureCount,
                RetryCount = aggregated.RetryCount,
                RevokedCount = aggregated.RevokedCount,
                AverageExecutionTime = aggregated.AverageExecutionTime,
                TasksPerSecond = aggregated.TasksPerSecond,
                ByTaskName = byTaskName.ToDictionary(
                    pair => pair.Key,
                    pair => new TaskMetrics
                    {
                        TaskName = pair.Value.TaskName,
                        TotalCount = pair.Value.TotalCount,
                        SuccessCount = pair.Value.SuccessCount,
                        FailureCount = pair.Value.FailureCount,
                        AverageExecutionTime = pair.Value.AverageExecutionTime,
                    },
                    StringComparer.Ordinal
                ),
            };
        }

        return await ComputeFromRecordsAsync(window, from, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<DashboardMetrics> ComputeFromRecordsAsync(
        TimeSpan window,
        DateTimeOffset from,
        CancellationToken cancellationToken
    )
    {
        long success = 0;
        long failure = 0;
        long retry = 0;
        long revoked = 0;
        var durations = new List<TimeSpan>();
        var byTaskName = new Dictionary<string, TaskAccumulator>(StringComparer.Ordinal);

        await foreach (
            var record in _records
                .QueryRecentAsync(from, WindowScanLimit, cancellationToken)
                .ConfigureAwait(false)
        )
        {
            if (!Enum.TryParse<TaskState>(record.State, out var state) || !IsCounted(state))
            {
                continue;
            }

            if (!byTaskName.TryGetValue(record.TaskName, out var accumulator))
            {
                accumulator = new TaskAccumulator();
                byTaskName[record.TaskName] = accumulator;
            }

            accumulator.Total++;

            switch (state)
            {
                case TaskState.Success:
                    success++;
                    accumulator.Success++;
                    if (record.Duration is { } duration)
                    {
                        durations.Add(duration);
                        accumulator.Durations.Add(duration);
                    }
                    break;
                case TaskState.Failure:
                    failure++;
                    accumulator.Failure++;
                    break;
                case TaskState.Retry:
                    retry++;
                    break;
                case TaskState.Revoked:
                    revoked++;
                    break;
            }
        }

        var total = success + failure;

        return new DashboardMetrics
        {
            Window = window,
            TotalProcessed = total,
            SuccessCount = success,
            FailureCount = failure,
            RetryCount = retry,
            RevokedCount = revoked,
            AverageExecutionTime = durations.Count > 0 ? Average(durations) : null,
            TasksPerSecond = window.TotalSeconds > 0 ? total / window.TotalSeconds : 0,
            ByTaskName = byTaskName.ToDictionary(
                pair => pair.Key,
                pair => new TaskMetrics
                {
                    TaskName = pair.Key,
                    TotalCount = pair.Value.Total,
                    SuccessCount = pair.Value.Success,
                    FailureCount = pair.Value.Failure,
                    AverageExecutionTime =
                        pair.Value.Durations.Count > 0 ? Average(pair.Value.Durations) : null,
                },
                StringComparer.Ordinal
            ),
        };
    }

    private static bool IsCounted(TaskState state) =>
        state is TaskState.Success or TaskState.Failure or TaskState.Retry or TaskState.Revoked;

    private static TimeSpan Average(List<TimeSpan> durations) =>
        TimeSpan.FromTicks((long)durations.Average(duration => duration.Ticks));

    private sealed class TaskAccumulator
    {
        public long Total { get; set; }

        public long Success { get; set; }

        public long Failure { get; set; }

        public List<TimeSpan> Durations { get; } = [];
    }
}
