using DotCelery.Core.Models;

namespace DotCelery.Core.Canvas;

/// <summary>
/// Result of executing a canvas workflow.
/// </summary>
public sealed class CanvasResult
{
    /// <summary>
    /// Gets the unique ID for this canvas execution.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the type of canvas that was executed.
    /// </summary>
    public required CanvasType Type { get; init; }

    /// <summary>
    /// Gets the task IDs for all tasks in the canvas.
    /// </summary>
    public required IReadOnlyList<string> TaskIds { get; init; }

    /// <summary>
    /// Gets the parent task ID (for tracking in workflows).
    /// </summary>
    public string? ParentId { get; init; }
}

/// <summary>
/// Result of executing a chain workflow.
/// </summary>
public sealed class ChainResult
{
    /// <summary>
    /// Gets the unique ID for this chain execution.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the task ID of the first task in the chain.
    /// </summary>
    public required string FirstTaskId { get; init; }

    /// <summary>
    /// Gets the task ID of the last task in the chain.
    /// </summary>
    public required string LastTaskId { get; init; }

    /// <summary>
    /// Gets all task IDs in execution order.
    /// </summary>
    public required IReadOnlyList<string> TaskIds { get; init; }
}

/// <summary>
/// Result of executing a group workflow.
/// </summary>
public sealed class GroupResult
{
    /// <summary>
    /// Gets the unique ID for this group execution.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets all task IDs in the group.
    /// </summary>
    public required IReadOnlyList<string> TaskIds { get; init; }

    /// <summary>
    /// Gets the number of tasks in the group.
    /// </summary>
    public int Count => TaskIds.Count;
}

/// <summary>
/// Result of executing a chord workflow.
/// </summary>
public sealed class ChordResult
{
    /// <summary>
    /// Gets the unique ID for this chord execution.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Gets the group result (header tasks).
    /// </summary>
    public required GroupResult Header { get; init; }

    /// <summary>
    /// Gets the callback task ID.
    /// </summary>
    public required string CallbackTaskId { get; init; }
}
