namespace DotCelery.Core.Canvas;

/// <summary>
/// Represents a chord - a group of tasks followed by a callback.
/// The callback receives the results of all tasks in the group.
/// </summary>
public sealed class Chord : CanvasPrimitive
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Chord"/> class.
    /// </summary>
    /// <param name="header">The group of tasks to execute in parallel.</param>
    /// <param name="callback">The callback to execute with all results.</param>
    public Chord(Group header, Signature callback)
    {
        ArgumentNullException.ThrowIfNull(header);
        ArgumentNullException.ThrowIfNull(callback);

        Header = header;
        Callback = callback;
    }

    /// <summary>
    /// Gets the header group (tasks to execute in parallel).
    /// </summary>
    public Group Header { get; }

    /// <summary>
    /// Gets the callback signature.
    /// </summary>
    public Signature Callback { get; }

    /// <summary>
    /// Gets the number of tasks in the header.
    /// </summary>
    public int Count => Header.Count;

    /// <inheritdoc />
    public override CanvasType Type => CanvasType.Chord;

    /// <inheritdoc />
    public override IEnumerable<Signature> GetSignatures()
    {
        foreach (var sig in Header.GetSignatures())
        {
            yield return sig;
        }

        yield return Callback;
    }
}
