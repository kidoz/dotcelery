using System.Diagnostics;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Filters;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that tracks queue metrics for executed tasks.
/// </summary>
public sealed class QueueMetricsFilter : ITaskFilterWithExceptionHandling
{
    private readonly IQueueMetrics _metrics;
    private readonly ILogger<QueueMetricsFilter> _logger;

    private const string StartTimeProperty = "QueueMetrics_StartTime";
    private const string RecordedProperty = "QueueMetrics_Recorded";

    /// <summary>
    /// Initializes a new instance of the <see cref="QueueMetricsFilter"/> class.
    /// </summary>
    public QueueMetricsFilter(IQueueMetrics metrics, ILogger<QueueMetricsFilter> logger)
    {
        _metrics = metrics;
        _logger = logger;
    }

    /// <inheritdoc />
    public int Order => -3000; // Run very early to track start accurately

    /// <inheritdoc />
    public async ValueTask OnExecutingAsync(
        TaskExecutingContext context,
        CancellationToken cancellationToken
    )
    {
        var queue = context.TaskContext.Queue;
        context.Properties[StartTimeProperty] = Stopwatch.GetTimestamp();
        context.Properties[RecordedProperty] = true;

        await _metrics
            .RecordStartedAsync(queue, context.TaskId, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Recorded task start for {TaskId} on queue {Queue}",
            context.TaskId,
            queue
        );
    }

    /// <inheritdoc />
    public async ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    )
    {
        if (
            !context.Properties.TryGetValue(RecordedProperty, out var recorded)
            || recorded is not true
        )
        {
            return;
        }

        var queue = context.TaskContext.Queue;
        var duration = context.Duration;

        // Calculate more accurate duration if we recorded start time
        if (
            context.Properties.TryGetValue(StartTimeProperty, out var startObj)
            && startObj is long startTime
        )
        {
            var elapsedTicks = Stopwatch.GetTimestamp() - startTime;
            duration = TimeSpan.FromTicks(
                (long)(elapsedTicks * ((double)TimeSpan.TicksPerSecond / Stopwatch.Frequency))
            );
        }

        await _metrics
            .RecordCompletedAsync(
                queue,
                context.TaskId,
                success: context.Succeeded,
                duration,
                cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Recorded task completion for {TaskId} on queue {Queue}, success={Success}, duration={Duration}ms",
            context.TaskId,
            queue,
            context.Succeeded,
            duration.TotalMilliseconds
        );
    }

    /// <inheritdoc />
    public async ValueTask<bool> OnExceptionAsync(
        TaskExceptionContext context,
        CancellationToken cancellationToken
    )
    {
        if (
            !context.Properties.TryGetValue(RecordedProperty, out var recorded)
            || recorded is not true
        )
        {
            return false;
        }

        var queue = context.TaskContext.Queue;
        var duration = TimeSpan.Zero;

        if (
            context.Properties.TryGetValue(StartTimeProperty, out var startObj)
            && startObj is long startTime
        )
        {
            var elapsedTicks = Stopwatch.GetTimestamp() - startTime;
            duration = TimeSpan.FromTicks(
                (long)(elapsedTicks * ((double)TimeSpan.TicksPerSecond / Stopwatch.Frequency))
            );
        }

        await _metrics
            .RecordCompletedAsync(
                queue,
                context.TaskId,
                success: false,
                duration,
                cancellationToken
            )
            .ConfigureAwait(false);

        _logger.LogDebug(
            "Recorded task failure for {TaskId} on queue {Queue}, duration={Duration}ms",
            context.TaskId,
            queue,
            duration.TotalMilliseconds
        );

        return false; // Don't handle the exception
    }
}
