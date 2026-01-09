namespace DotCelery.Core.Canvas;

/// <summary>
/// Represents a group of tasks executed in parallel.
/// All tasks in the group run concurrently and their results are collected.
/// </summary>
public sealed class Group : CanvasPrimitive
{
    private readonly List<CanvasPrimitive> _members;

    /// <summary>
    /// Initializes a new instance of the <see cref="Group"/> class.
    /// </summary>
    /// <param name="signatures">The signatures to execute in parallel.</param>
    public Group(IEnumerable<Signature> signatures)
    {
        ArgumentNullException.ThrowIfNull(signatures);
        _members = signatures.Select(s => (CanvasPrimitive)new SignatureWrapper(s)).ToList();

        if (_members.Count == 0)
        {
            throw new ArgumentException(
                "Group must contain at least one task.",
                nameof(signatures)
            );
        }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Group"/> class using params span (zero-allocation).
    /// </summary>
    /// <param name="signatures">The signatures to execute in parallel.</param>
    public Group(params ReadOnlySpan<Signature> signatures)
    {
        if (signatures.IsEmpty)
        {
            throw new ArgumentException(
                "Group must contain at least one task.",
                nameof(signatures)
            );
        }

        _members = new List<CanvasPrimitive>(signatures.Length);
        foreach (var signature in signatures)
        {
            _members.Add(new SignatureWrapper(signature));
        }
    }

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

        _members = new List<CanvasPrimitive>(primitives.Length);
        foreach (var primitive in primitives)
        {
            _members.Add(primitive);
        }
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
    /// Adds another signature to run in parallel.
    /// </summary>
    /// <param name="signature">The signature to add.</param>
    /// <returns>This group for fluent chaining.</returns>
    public Group And(Signature signature)
    {
        ArgumentNullException.ThrowIfNull(signature);
        _members.Add(new SignatureWrapper(signature));
        return this;
    }

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
    /// Creates a chain where this group is followed by a signature.
    /// </summary>
    /// <param name="signature">The next task to execute.</param>
    /// <returns>A chain containing this group followed by the signature.</returns>
    public Chain Then(Signature signature)
    {
        // This is effectively a chord - run group, then run signature with results
        return new Chain([GroupSignature.Create(this), signature]);
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

/// <summary>
/// Wrapper to treat a Signature as a CanvasPrimitive.
/// </summary>
internal sealed class SignatureWrapper : CanvasPrimitive
{
    public SignatureWrapper(Signature signature)
    {
        Signature = signature;
    }

    public Signature Signature { get; }

    public override CanvasType Type => CanvasType.Signature;

    public override IEnumerable<Signature> GetSignatures()
    {
        yield return Signature;
    }
}

/// <summary>
/// Internal signature representing a group result.
/// </summary>
internal sealed class GroupSignature : Signature
{
    public GroupSignature(Group group)
        : base()
    {
        Group = group;
    }

    public Group Group { get; }

    /// <summary>
    /// Creates a new GroupSignature.
    /// </summary>
    public static GroupSignature Create(Group group) => new(group) { TaskName = "__group__" };
}
