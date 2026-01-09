using System.Collections.Frozen;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Attributes;
using DotCelery.Core.Filters;
using DotCelery.Core.RateLimiting;
using DotCelery.Core.Routing;
using DotCelery.Core.TimeLimits;
using Microsoft.Extensions.Logging;

namespace DotCelery.Worker.Registry;

/// <summary>
/// Registry of available tasks for the worker.
/// </summary>
public sealed class TaskRegistry
{
    private readonly Lock _lock = new();
    private readonly ILogger<TaskRegistry>? _logger;

    private FrozenDictionary<string, TaskRegistration> _tasks = FrozenDictionary<
        string,
        TaskRegistration
    >.Empty;

    /// <summary>
    /// Gets or sets whether to throw an exception when a duplicate task registration is attempted.
    /// When false (default), duplicate registrations log a warning and overwrite the previous registration.
    /// </summary>
    public bool ThrowOnDuplicateRegistration { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskRegistry"/> class.
    /// </summary>
    public TaskRegistry() { }

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskRegistry"/> class with logging support.
    /// </summary>
    /// <param name="logger">The logger for duplicate registration warnings.</param>
    public TaskRegistry(ILogger<TaskRegistry>? logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Registers a task type.
    /// </summary>
    /// <typeparam name="TTask">The task type.</typeparam>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a task with the same name is already registered and <see cref="ThrowOnDuplicateRegistration"/> is true.
    /// </exception>
    public void Register<TTask>()
        where TTask : ITask
    {
        var taskName = TTask.TaskName;
        var taskType = typeof(TTask);

        RegisterInternal(taskType, taskName);
    }

    /// <summary>
    /// Registers a task type by type.
    /// </summary>
    /// <param name="taskType">The task type.</param>
    /// <param name="taskName">The task name.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a task with the same name is already registered and <see cref="ThrowOnDuplicateRegistration"/> is true.
    /// </exception>
    public void Register(Type taskType, string taskName)
    {
        RegisterInternal(taskType, taskName);
    }

    private void RegisterInternal(Type taskType, string taskName)
    {
        var registration = new TaskRegistration(
            TaskName: taskName,
            TaskType: taskType,
            InputType: GetInputType(taskType),
            OutputType: GetOutputType(taskType),
            RateLimitPolicy: GetRateLimitPolicy(taskType),
            FilterTypes: GetFilterTypes(taskType),
            Queue: GetQueue(taskType),
            TimeLimitPolicy: GetTimeLimitPolicy(taskType)
        );

        lock (_lock)
        {
            var builder = _tasks.ToDictionary();

            if (builder.TryGetValue(taskName, out var existingRegistration))
            {
                if (existingRegistration.TaskType == taskType)
                {
                    // Same type registered twice - silently skip
                    return;
                }

                var message =
                    $"Duplicate task registration for '{taskName}'. "
                    + $"Existing type: {existingRegistration.TaskType.FullName}, "
                    + $"New type: {taskType.FullName}. "
                    + "The new registration will overwrite the existing one.";

                if (ThrowOnDuplicateRegistration)
                {
                    throw new InvalidOperationException(message);
                }

                _logger?.LogWarning("{Message}", message);
            }

            builder[taskName] = registration;
            _tasks = builder.ToFrozenDictionary();
        }
    }

    /// <summary>
    /// Gets task registration by name.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <returns>The registration, or null if not found.</returns>
    public TaskRegistration? GetTask(string taskName)
    {
        return _tasks.GetValueOrDefault(taskName);
    }

    /// <summary>
    /// Gets all registered tasks.
    /// </summary>
    /// <returns>All task registrations.</returns>
    public IReadOnlyDictionary<string, TaskRegistration> GetAllTasks() => _tasks;

    /// <summary>
    /// Checks if a task is registered.
    /// </summary>
    /// <param name="taskName">The task name.</param>
    /// <returns>True if registered.</returns>
    public bool IsRegistered(string taskName) => _tasks.ContainsKey(taskName);

    private static Type? GetInputType(Type taskType)
    {
        var iface = taskType
            .GetInterfaces()
            .FirstOrDefault(i =>
                i.IsGenericType
                && (
                    i.GetGenericTypeDefinition() == typeof(ITask<>)
                    || i.GetGenericTypeDefinition() == typeof(ITask<,>)
                )
            );

        return iface?.GetGenericArguments().FirstOrDefault();
    }

    private static Type? GetOutputType(Type taskType)
    {
        var iface = taskType
            .GetInterfaces()
            .FirstOrDefault(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ITask<,>)
            );

        return iface?.GetGenericArguments().ElementAtOrDefault(1);
    }

    private static RateLimitPolicy? GetRateLimitPolicy(Type taskType)
    {
        var attribute = taskType
            .GetCustomAttributes(typeof(RateLimitAttribute), inherit: true)
            .OfType<RateLimitAttribute>()
            .FirstOrDefault();

        return attribute?.ToPolicy();
    }

    private static List<Type>? GetFilterTypes(Type taskType)
    {
        var attributes = taskType
            .GetCustomAttributes(typeof(TaskFilterAttribute), inherit: true)
            .OfType<TaskFilterAttribute>()
            .OrderBy(a => a.Order)
            .ToList();

        if (attributes.Count == 0)
        {
            return null;
        }

        return attributes.Select(a => a.FilterType).ToList();
    }

    private static string? GetQueue(Type taskType)
    {
        var attribute = taskType
            .GetCustomAttributes(typeof(RouteAttribute), inherit: true)
            .OfType<RouteAttribute>()
            .FirstOrDefault();

        return attribute?.Queue;
    }

    private static TimeLimitPolicy? GetTimeLimitPolicy(Type taskType)
    {
        var attribute = taskType
            .GetCustomAttributes(typeof(TimeLimitAttribute), inherit: true)
            .OfType<TimeLimitAttribute>()
            .FirstOrDefault();

        return attribute?.ToPolicy();
    }
}
