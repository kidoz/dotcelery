namespace DotCelery.Core.Canvas;

/// <summary>
/// Represents a group of tasks executed in parallel.
/// All tasks in the group run concurrently and their results are collected.
/// </summary>
/// <remarks>
/// A member is any canvas primitive: a signature, a chain, or another group. A nested group
/// is flattened into its members by the client that sends the canvas.
/// </remarks>
public sealed class Group : CanvasPrimitive
{
    private readonly List<CanvasPrimitive> _members;

    /// <summary>
    /// Initializes a new instance of the <see cref="Group"/> class with canvas primitives.
    /// </summary>
    /// <param name="primitives">The primitives to execute in parallel.</param>
    public Group(IEnumerable<CanvasPrimitive> primitives)
    {
        ArgumentNullException.ThrowIfNull(primitives);
        _members = [.. primitives];

        if (_members.Count == 0)
        {
            throw new ArgumentException(
                "Group must contain at least one primitive.",
                nameof(primitives)
            );
        }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Group"/> class with canvas primitives using params span.
    /// </summary>
    /// <param name="primitives">The primitives to execute in parallel.</param>
    public Group(params ReadOnlySpan<CanvasPrimitive> primitives)
    {
        if (primitives.IsEmpty)
        {
            throw new ArgumentException(
                "Group must contain at least one primitive.",
                nameof(primitives)
            );
        }

        _members = [.. primitives];
    }

    /// <summary>
    /// Gets the members of the group.
    /// </summary>
    public IReadOnlyList<CanvasPrimitive> Members => _members;

    /// <summary>
    /// Gets whether this group is empty.
    /// </summary>
    public bool IsEmpty => _members.Count == 0;

    /// <summary>
    /// Gets whether this group has a single member.
    /// </summary>
    public bool IsSingle => _members.Count == 1;

    /// <summary>
    /// Gets the total number of signatures including nested primitives.
    /// </summary>
    public int TotalSignatureCount => GetSignatures().Count();

    /// <summary>
    /// Gets the number of members in the group.
    /// </summary>
    public int Count => _members.Count;

    /// <summary>
    /// Adds another primitive to run in parallel.
    /// </summary>
    /// <param name="primitive">The primitive to add.</param>
    /// <returns>This group for fluent chaining.</returns>
    public Group And(CanvasPrimitive primitive)
    {
        ArgumentNullException.ThrowIfNull(primitive);
        _members.Add(primitive);
        return this;
    }

    /// <summary>
    /// Creates a chord with this group and a callback.
    /// </summary>
    /// <param name="callback">The callback signature to execute after all group tasks complete.</param>
    /// <returns>A chord combining the group and callback.</returns>
    public Chord WithCallback(Signature callback)
    {
        return new Chord(this, callback);
    }

    /// <summary>
    /// Creates a chord where this group runs, then the signature follows it: the workflow-named
    /// form of <see cref="WithCallback"/>.
    /// </summary>
    /// <param name="signature">The task to run after the group.</param>
    /// <returns>A chord combining the group and the signature.</returns>
    public Chord Then(Signature signature)
    {
        // A group is followed by a task through the batch callback that waits for its members, so
        // this is a chord and not a chain: a chain step would have to publish the group itself
        return WithCallback(signature);
    }

    /// <summary>
    /// Adds a signature to a group.
    /// </summary>
    /// <param name="group">The group to extend.</param>
    /// <param name="signature">The signature to add.</param>
    /// <returns>The updated group.</returns>
    public static Group operator |(Group group, Signature signature)
    {
        group.And(signature);
        return group;
    }

    /// <summary>
    /// Combines a signature with a group.
    /// </summary>
    /// <param name="signature">The signature to add.</param>
    /// <param name="group">The group to extend.</param>
    /// <returns>The updated group.</returns>
    public static Group operator |(Signature signature, Group group)
    {
        group.And(signature);
        return group;
    }

    /// <inheritdoc />
    public override CanvasType Type => CanvasType.Group;

    /// <inheritdoc />
    public override IEnumerable<Signature> GetSignatures()
    {
        return _members.SelectMany(m => m.GetSignatures());
    }
}
