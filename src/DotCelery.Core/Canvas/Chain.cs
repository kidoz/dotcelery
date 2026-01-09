namespace DotCelery.Core.Canvas;

/// <summary>
/// Represents a chain of tasks executed sequentially.
/// Each task's result is passed to the next task in the chain.
/// </summary>
public sealed class Chain : CanvasPrimitive
{
    private readonly List<Signature> _tasks;

    /// <summary>
    /// Initializes a new instance of the <see cref="Chain"/> class.
    /// </summary>
    /// <param name="tasks">The tasks to chain together.</param>
    public Chain(IEnumerable<Signature> tasks)
    {
        ArgumentNullException.ThrowIfNull(tasks);
        _tasks = [.. tasks];

        if (_tasks.Count == 0)
        {
            throw new ArgumentException("Chain must contain at least one task.", nameof(tasks));
        }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Chain"/> class using params span (zero-allocation).
    /// </summary>
    /// <param name="tasks">The tasks to chain together.</param>
    public Chain(params ReadOnlySpan<Signature> tasks)
    {
        if (tasks.IsEmpty)
        {
            throw new ArgumentException("Chain must contain at least one task.", nameof(tasks));
        }

        _tasks = new List<Signature>(tasks.Length);
        foreach (var task in tasks)
        {
            _tasks.Add(task);
        }
    }

    /// <summary>
    /// Gets the tasks in the chain.
    /// </summary>
    public IReadOnlyList<Signature> Tasks => _tasks;

    /// <summary>
    /// Gets whether this chain is empty.
    /// </summary>
    public bool IsEmpty => _tasks.Count == 0;

    /// <summary>
    /// Gets whether this chain has a single task.
    /// </summary>
    public bool IsSingle => _tasks.Count == 1;

    /// <summary>
    /// Gets the total number of tasks.
    /// </summary>
    public int TotalTaskCount => _tasks.Count;

    /// <summary>
    /// Gets the first task in the chain.
    /// </summary>
    public Signature First => _tasks[0];

    /// <summary>
    /// Gets the last task in the chain.
    /// </summary>
    public Signature Last => _tasks[^1];

    /// <summary>
    /// Appends a task to the end of the chain.
    /// </summary>
    /// <param name="signature">The task signature to append.</param>
    /// <returns>This chain for fluent chaining.</returns>
    public Chain Then(Signature signature)
    {
        ArgumentNullException.ThrowIfNull(signature);
        _tasks.Add(signature);
        return this;
    }

    /// <summary>
    /// Appends another chain to the end of this chain.
    /// </summary>
    /// <param name="other">The chain to append.</param>
    /// <returns>This chain for fluent chaining.</returns>
    public Chain Then(Chain other)
    {
        ArgumentNullException.ThrowIfNull(other);
        _tasks.AddRange(other._tasks);
        return this;
    }

    /// <summary>
    /// Appends a signature to a chain.
    /// </summary>
    /// <param name="chain">The chain to extend.</param>
    /// <param name="signature">The signature to append.</param>
    /// <returns>The updated chain.</returns>
    public static Chain operator +(Chain chain, Signature signature)
    {
        chain.Then(signature);
        return chain;
    }

    /// <summary>
    /// Concatenates two chains.
    /// </summary>
    /// <param name="left">The chain to extend.</param>
    /// <param name="right">The chain to append.</param>
    /// <returns>The updated chain.</returns>
    public static Chain operator +(Chain left, Chain right)
    {
        left.Then(right);
        return left;
    }

    /// <summary>
    /// Creates a chord with this chain and a callback.
    /// Note: For chains, this executes the chain then the callback.
    /// </summary>
    /// <param name="callback">The callback signature.</param>
    /// <returns>A chord combining the chain and callback.</returns>
    public Chord WithCallback(Signature callback)
    {
        // For a chain, we convert to a group of one chain
        return new Chord(new Group([this]), callback);
    }

    /// <inheritdoc />
    public override CanvasType Type => CanvasType.Chain;

    /// <inheritdoc />
    public override IEnumerable<Signature> GetSignatures() => _tasks;
}
