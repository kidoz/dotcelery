namespace DotCelery.Core.Attributes;

/// <summary>
/// Marks a task to prevent overlapping execution.
/// When applied, only one instance of the task can run at a time.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class PreventOverlappingAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the timeout after which the lock is automatically released.
    /// Default is 1 hour.
    /// </summary>
    public int TimeoutSeconds { get; set; } = 3600;

    /// <summary>
    /// Gets or sets whether to use input-based keying.
    /// When true, different inputs can run concurrently but same inputs cannot.
    /// Default is false (task-level locking).
    /// </summary>
    public bool KeyByInput { get; set; }

    /// <summary>
    /// Gets or sets the property name to use as the key when KeyByInput is true.
    /// If not specified, a hash of the entire input is used.
    /// </summary>
    public string? KeyProperty { get; set; }
}
