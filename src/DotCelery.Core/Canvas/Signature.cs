using System.Diagnostics.CodeAnalysis;
using DotCelery.Core.Abstractions;

namespace DotCelery.Core.Canvas;

/// <summary>
/// Represents a task signature - a blueprint for a task invocation.
/// </summary>
public class Signature
{
    /// <summary>
    /// Gets or sets the task name.
    /// </summary>
    public required string TaskName { get; init; }

    /// <summary>
    /// Gets or sets the serialized task arguments.
    /// </summary>
    public byte[]? Args { get; init; }

    /// <summary>
    /// Gets or sets the target queue.
    /// </summary>
    public string Queue { get; init; } = "celery";

    /// <summary>
    /// Gets or sets the task priority.
    /// </summary>
    public int Priority { get; init; }

    /// <summary>
    /// Gets or sets the maximum retries.
    /// </summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>
    /// Gets or sets the countdown before execution.
    /// </summary>
    public TimeSpan? Countdown { get; init; }

    /// <summary>
    /// Gets or sets the scheduled execution time.
    /// </summary>
    public DateTimeOffset? Eta { get; init; }

    /// <summary>
    /// Gets or sets the expiration time.
    /// </summary>
    public DateTimeOffset? Expires { get; init; }

    /// <summary>
    /// Gets or sets custom headers.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>
    /// Gets or sets whether to store the result.
    /// </summary>
    public bool StoreResult { get; init; } = true;

    /// <summary>
    /// Gets whether this signature has a linked next task.
    /// </summary>
    public bool HasLink => Link is not null;

    /// <summary>
    /// Gets whether this signature has an error callback.
    /// </summary>
    public bool HasErrorLink => LinkError is not null;

    /// <summary>
    /// Gets whether this signature has a scheduled execution time (ETA).
    /// </summary>
    public bool HasEta => Eta.HasValue;

    /// <summary>
    /// Gets whether this signature has a countdown delay.
    /// </summary>
    public bool HasCountdown => Countdown.HasValue;

    /// <summary>
    /// Gets whether this signature is scheduled (has ETA or countdown).
    /// </summary>
    public bool IsScheduled => Eta.HasValue || Countdown.HasValue;

    /// <summary>
    /// Gets whether this signature has expired based on the current time.
    /// </summary>
    public bool IsExpired => Expires.HasValue && Expires.Value < DateTimeOffset.UtcNow;

    /// <summary>
    /// Gets whether this signature has custom headers.
    /// </summary>
    public bool HasHeaders => Headers is { Count: > 0 };

    /// <summary>
    /// Gets the effective execution time (ETA or now + countdown).
    /// </summary>
    public DateTimeOffset? EffectiveEta =>
        Eta ?? (Countdown.HasValue ? DateTimeOffset.UtcNow + Countdown.Value : null);

    /// <summary>
    /// Gets the time remaining until execution, or null if not scheduled.
    /// </summary>
    public TimeSpan? TimeUntilExecution
    {
        get
        {
            var eta = EffectiveEta;
            if (eta is null)
            {
                return null;
            }

            var remaining = eta.Value - DateTimeOffset.UtcNow;
            return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
        }
    }

    /// <summary>
    /// Gets the linked next task signature for chaining.
    /// </summary>
    public Signature? Link { get; init; }

    /// <summary>
    /// Gets the error callback signature.
    /// </summary>
    public Signature? LinkError { get; init; }

    /// <summary>
    /// Creates a chain with this signature followed by another.
    /// </summary>
    /// <param name="other">The next signature in the chain.</param>
    /// <returns>A chain containing both signatures.</returns>
    public Chain Then(Signature other)
    {
        return new Chain([this, other]);
    }

    /// <summary>
    /// Creates a group with this signature and another running in parallel.
    /// </summary>
    /// <param name="other">Another signature to run in parallel.</param>
    /// <returns>A group containing both signatures.</returns>
    public Group And(Signature other)
    {
        return new Group([this, other]);
    }

    /// <summary>
    /// Combines two signatures into a chain (sequential execution).
    /// </summary>
    /// <param name="left">The first signature.</param>
    /// <param name="right">The second signature.</param>
    /// <returns>A chain containing both signatures.</returns>
    public static Chain operator +(Signature left, Signature right) => new([left, right]);

    /// <summary>
    /// Combines two signatures into a group (parallel execution).
    /// </summary>
    /// <param name="left">The first signature.</param>
    /// <param name="right">The second signature.</param>
    /// <returns>A group containing both signatures.</returns>
    public static Group operator |(Signature left, Signature right) => new([left, right]);
}

/// <summary>
/// Typed signature for a specific task.
/// </summary>
/// <typeparam name="TTask">The task type.</typeparam>
/// <typeparam name="TInput">The input type.</typeparam>
/// <typeparam name="TOutput">The output type.</typeparam>
public sealed class Signature<TTask, TInput, TOutput> : Signature
    where TTask : ITask<TInput, TOutput>
    where TInput : class
    where TOutput : class
{
    /// <summary>
    /// Initializes a new instance of the signature.
    /// </summary>
    [SetsRequiredMembers]
    public Signature()
    {
        TaskName = TTask.TaskName;
    }

    /// <summary>
    /// Gets or sets the strongly-typed input.
    /// </summary>
    public TInput? Input { get; init; }
}

/// <summary>
/// Typed signature for a task with no return value.
/// </summary>
/// <typeparam name="TTask">The task type.</typeparam>
/// <typeparam name="TInput">The input type.</typeparam>
public sealed class Signature<TTask, TInput> : Signature
    where TTask : ITask<TInput>
    where TInput : class
{
    /// <summary>
    /// Initializes a new instance of the signature.
    /// </summary>
    [SetsRequiredMembers]
    public Signature()
    {
        TaskName = TTask.TaskName;
    }

    /// <summary>
    /// Gets or sets the strongly-typed input.
    /// </summary>
    public TInput? Input { get; init; }
}
