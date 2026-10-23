namespace DotCelery.Core.Filters;

/// <summary>
/// Specifies a task filter to apply to a task class.
/// Multiple attributes can be applied to chain filters.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = true)]
public sealed class TaskFilterAttribute : Attribute
{
    /// <summary>
    /// Gets the filter type. Must implement <see cref="ITaskFilter"/> or <see cref="ITaskExceptionFilter"/>.
    /// </summary>
    public Type FilterType { get; }

    /// <summary>
    /// Gets or sets the order of this filter relative to other filters.
    /// Lower values execute first on entry, last on exit.
    /// Default is 0.
    /// </summary>
    public int Order { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskFilterAttribute"/> class.
    /// </summary>
    /// <param name="filterType">The filter type. Must implement <see cref="ITaskFilter"/> or <see cref="ITaskExceptionFilter"/>.</param>
    /// <exception cref="ArgumentNullException">Thrown when filterType is null.</exception>
    /// <exception cref="ArgumentException">Thrown when filterType does not implement required interfaces.</exception>
    public TaskFilterAttribute(Type filterType)
    {
        ArgumentNullException.ThrowIfNull(filterType);

        if (
            !typeof(ITaskFilter).IsAssignableFrom(filterType)
            && !typeof(ITaskExceptionFilter).IsAssignableFrom(filterType)
        )
        {
            throw new ArgumentException(
                $"Filter type must implement {nameof(ITaskFilter)} or {nameof(ITaskExceptionFilter)}",
                nameof(filterType)
            );
        }

        FilterType = filterType;
    }
}
