using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text.Json;
using DotCelery.Core.Attributes;
using DotCelery.Core.Execution;
using DotCelery.Core.Filters;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that prevents overlapping execution of tasks marked with <see cref="PreventOverlappingAttribute"/>.
/// </summary>
public sealed class PreventOverlappingFilter
    : ITaskFilterWithExceptionHandling,
        IAsyncDisposable,
        IExecutionResourceFilter
{
    private readonly ITaskExecutionTracker _tracker;
    private readonly ILogger<PreventOverlappingFilter> _logger;

    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<ExecutionLease, byte> _leases = new();

    private const string TrackedProperty = "PreventOverlapping_Tracked";

    /// <summary>
    /// Initializes a new instance of the <see cref="PreventOverlappingFilter"/> class.
    /// </summary>
    public PreventOverlappingFilter(
        ITaskExecutionTracker tracker,
        ILogger<PreventOverlappingFilter> logger
    )
        : this(tracker, logger, TimeProvider.System) { }

    /// <summary>
    /// Initializes the filter with the clock used to renew execution leases.
    /// </summary>
    public PreventOverlappingFilter(
        ITaskExecutionTracker tracker,
        ILogger<PreventOverlappingFilter> logger,
        TimeProvider timeProvider
    )
    {
        _tracker = tracker;
        _logger = logger;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public int Order => -900; // Run early, after partitioning

    /// <inheritdoc />
    public async ValueTask OnExecutingAsync(
        TaskExecutingContext context,
        CancellationToken cancellationToken
    )
    {
        var attribute = context
            .TaskType.GetCustomAttributes(typeof(PreventOverlappingAttribute), true)
            .OfType<PreventOverlappingAttribute>()
            .FirstOrDefault();

        if (attribute is null)
        {
            return; // Task doesn't have the attribute
        }

        var key = GetKey(context, attribute);

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(attribute.TimeoutSeconds);
        var timeout = TimeSpan.FromSeconds(attribute.TimeoutSeconds);

        var started = await _tracker
            .TryStartAsync(context.TaskName, context.TaskId, key, timeout, cancellationToken)
            .ConfigureAwait(false);

        if (!started)
        {
            var existingTaskId = await _tracker
                .GetExecutingTaskIdAsync(context.TaskName, key, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "Skipping task {TaskId} ({TaskName}) - another instance {ExistingTaskId} is already running",
                context.TaskId,
                context.TaskName,
                existingTaskId
            );

            context.SkipExecution = true;
            context.RequeueMessage = true;
            context.RequeueDelay = TimeSpan.FromSeconds(1);
            context.Properties["SkippedDueToOverlap"] = true;
            return;
        }

        var lease = new ExecutionLease(
            _tracker,
            context.TaskName,
            context.TaskId,
            key,
            timeout,
            _timeProvider,
            cancellationToken
        );
        _leases.TryAdd(lease, 0);
        context.Properties[TrackedProperty] = lease;

        _logger.LogDebug(
            "Started execution tracking for task {TaskId} ({TaskName}), key={Key}",
            context.TaskId,
            context.TaskName,
            key ?? "(none)"
        );
    }

    /// <inheritdoc />
    public async ValueTask OnExecutedAsync(
        TaskExecutedContext context,
        CancellationToken cancellationToken
    )
    {
        try
        {
            await StopTrackingIfNeededAsync(
                    context.TaskId,
                    context.TaskName,
                    context.Properties,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // The pipeline logs thrown OnExecuted exceptions. Set the execution exception
            // explicitly so a lost lease cannot be recorded as a successful completion.
            context.Exception ??= ex;
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> OnExceptionAsync(
        TaskExceptionContext context,
        CancellationToken cancellationToken
    )
    {
        await StopTrackingIfNeededAsync(
                context.TaskId,
                context.TaskName,
                context.Properties,
                cancellationToken
            )
            .ConfigureAwait(false);

        return false; // Don't handle exception
    }

    private async ValueTask StopTrackingIfNeededAsync(
        string taskId,
        string taskName,
        IDictionary<string, object?> properties,
        CancellationToken cancellationToken
    )
    {
        if (
            properties.TryGetValue(TrackedProperty, out var value)
            && value is ExecutionLease lease
            && _leases.TryRemove(lease, out _)
        )
        {
            await lease.DisposeAsync().ConfigureAwait(false);
            _logger.LogDebug(
                "Stopped execution tracking for task {TaskId} ({TaskName})",
                taskId,
                taskName
            );
        }
    }

    ValueTask IExecutionResourceFilter.ReleaseAsync(TaskExecutingContext context) =>
        StopTrackingIfNeededAsync(
            context.TaskId,
            context.TaskName,
            context.Properties,
            CancellationToken.None
        );

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // Scope disposal also cleans leases when a later filter short-circuits or throws.
        foreach (var lease in _leases.Keys)
        {
            if (_leases.TryRemove(lease, out _))
            {
                await lease.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private sealed class ExecutionLease : IAsyncDisposable
    {
        private readonly ITaskExecutionTracker _tracker;
        private readonly string _taskName;
        private readonly string _taskId;
        private readonly string? _key;
        private readonly CancellationTokenSource _stop;
        private readonly Task _renewal;

        public ExecutionLease(
            ITaskExecutionTracker tracker,
            string taskName,
            string taskId,
            string? key,
            TimeSpan timeout,
            TimeProvider timeProvider,
            CancellationToken cancellationToken
        )
        {
            _tracker = tracker;
            _taskName = taskName;
            _taskId = taskId;
            _key = key;
            _stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _renewal = RenewAsync(timeout, timeProvider);
        }

        private async Task RenewAsync(TimeSpan timeout, TimeProvider timeProvider)
        {
            while (true)
            {
                await Task.Delay(timeout / 3, timeProvider, _stop.Token).ConfigureAwait(false);
                if (
                    !await _tracker
                        .ExtendAsync(_taskName, _taskId, _key, timeout, _stop.Token)
                        .ConfigureAwait(false)
                )
                {
                    throw new InvalidOperationException(
                        $"Execution lease for task {_taskId} was lost."
                    );
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            try
            {
                await _renewal.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
            finally
            {
                _stop.Dispose();
                await _tracker
                    .StopAsync(_taskName, _taskId, _key, CancellationToken.None)
                    .ConfigureAwait(false);
            }
        }
    }

    private static string? GetKey(
        TaskExecutingContext context,
        PreventOverlappingAttribute attribute
    )
    {
        if (!attribute.KeyByInput)
        {
            return null; // Task-level locking
        }

        if (context.Input is null)
        {
            return null; // No input to hash
        }

        if (!string.IsNullOrEmpty(attribute.KeyProperty))
        {
            // Use specific property as key
            return GetPropertyValue(context.Input, attribute.KeyProperty);
        }

        // Hash the entire input
        return ComputeInputHash(context.Input);
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "Keying by an input property reflects over the task input; UsePreventOverlapping is annotated as requiring unreferenced code, so a trimmed application that relies on this property preserves it."
    )]
    private static string? GetPropertyValue(object input, string propertyName)
    {
        try
        {
            var property = input.GetType().GetProperty(propertyName);
            if (property is not null)
            {
                var value = property.GetValue(input);
                return value?.ToString();
            }
        }
        catch
        {
            // Ignore reflection errors
        }

        return null;
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "Hashing the whole input serializes it with reflection; UsePreventOverlapping is annotated as requiring unreferenced code, so a Native AOT application does not use this mode."
    )]
    [UnconditionalSuppressMessage(
        "AOT",
        "IL3050",
        Justification = "Hashing the whole input serializes it with reflection; UsePreventOverlapping is annotated as requiring dynamic code, so a Native AOT application does not use this mode."
    )]
    private static string ComputeInputHash(object input)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(input);
        var hash = SHA256.HashData(json);
        return Convert.ToHexStringLower(hash)[..16]; // First 16 chars
    }
}

// Execution resources must be released even when a later filter short-circuits, including
// filters constructed by the pipeline rather than owned by the DI scope.
internal interface IExecutionResourceFilter
{
    ValueTask ReleaseAsync(TaskExecutingContext context);
}
