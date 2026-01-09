namespace DotCelery.Worker.Filters;

/// <summary>
/// Options for task filter configuration.
/// </summary>
public sealed class TaskFilterOptions
{
    /// <summary>
    /// Gets the list of global filter types that apply to all tasks.
    /// </summary>
    public List<Type> GlobalFilterTypes { get; } = [];
}
