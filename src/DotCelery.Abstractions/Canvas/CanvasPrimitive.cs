namespace DotCelery.Core.Canvas;

/// <summary>
/// Base class for canvas workflow primitives.
/// </summary>
public abstract class CanvasPrimitive
{
    /// <summary>
    /// Gets the type of this canvas primitive.
    /// </summary>
    public abstract CanvasType Type { get; }

    /// <summary>
    /// Gets all signatures contained in this primitive.
    /// </summary>
    /// <returns>An enumerable of all signatures.</returns>
    public abstract IEnumerable<Signature> GetSignatures();
}

/// <summary>
/// Types of canvas primitives.
/// </summary>
public enum CanvasType
{
    /// <summary>
    /// A single task signature.
    /// </summary>
    Signature,

    /// <summary>
    /// A chain of sequential tasks.
    /// </summary>
    Chain,

    /// <summary>
    /// A group of parallel tasks.
    /// </summary>
    Group,

    /// <summary>
    /// A chord (group + callback).
    /// </summary>
    Chord,
}
