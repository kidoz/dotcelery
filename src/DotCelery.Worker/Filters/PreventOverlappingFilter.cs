using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using DotCelery.Core.Attributes;
using DotCelery.Core.Execution;
using DotCelery.Core.Filters;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Filters;

/// <summary>
/// Filter that prevents overlapping execution of tasks marked with <see cref="PreventOverlappingAttribute"/>.
/// </summary>
public sealed class PreventOverlappingFilter : ITaskFilterWithExceptionHandling
{
    private readonly ITaskExecutionTracker _tracker;
    private readonly ILogger<PreventOverlappingFilter> _logger;

    private const string TrackedProperty = "PreventOverlapping_Tracked";
    private const string KeyProperty = "PreventOverlapping_Key";

    /// <summary>
    /// Initializes a new instance of the <see cref="PreventOverlappingFilter"/> class.
    /// </summary>
    public PreventOverlappingFilter(
        ITaskExecutionTracker tracker,
        ILogger<PreventOverlappingFilter> logger
    )
    {
        _tracker = tracker;
        _logger = logger;
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
        context.Properties[KeyProperty] = key ?? string.Empty;

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
            context.Properties["SkippedDueToOverlap"] = true;
            return;
        }

        context.Properties[TrackedProperty] = true;

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
        await StopTrackingIfNeededAsync(
                context.TaskId,
                context.TaskName,
                context.Properties,
                cancellationToken
            )
            .ConfigureAwait(false);
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
        if (!properties.TryGetValue(TrackedProperty, out var tracked) || tracked is not true)
        {
            return;
        }

        string? key = null;
        if (
            properties.TryGetValue(KeyProperty, out var keyObj)
            && keyObj is string keyStr
            && !string.IsNullOrEmpty(keyStr)
        )
        {
            key = keyStr;
        }

        await _tracker.StopAsync(taskName, taskId, key, cancellationToken).ConfigureAwait(false);

        _logger.LogDebug(
            "Stopped execution tracking for task {TaskId} ({TaskName})",
            taskId,
            taskName
        );
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

    private static string ComputeInputHash(object input)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(input);
        var hash = SHA256.HashData(json);
        return Convert.ToHexStringLower(hash)[..16]; // First 16 chars
    }
}
