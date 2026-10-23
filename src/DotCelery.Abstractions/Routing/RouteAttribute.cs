namespace DotCelery.Core.Routing;

/// <summary>
/// Specifies the queue for a task.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class RouteAttribute : Attribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RouteAttribute"/> class.
    /// </summary>
    /// <param name="queue">The target queue name.</param>
    public RouteAttribute(string queue)
    {
        Queue = queue ?? throw new ArgumentNullException(nameof(queue));
    }

    /// <summary>
    /// Gets the target queue name.
    /// </summary>
    public string Queue { get; }
}
