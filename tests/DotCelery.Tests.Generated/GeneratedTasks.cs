using DotCelery.Core.Abstractions;

namespace DotCelery.Tests.Generated;

/// <summary>
/// Tasks for the generated registration: a public one, an internal one, and a nested one, so the
/// generator is exercised through the real compiler.
/// </summary>
public sealed class FirstTask : ITask<OrderInput>
{
    /// <summary>Gets the name the task routes by.</summary>
    public static string TaskName => "generated.first";

    /// <inheritdoc />
    public Task ExecuteAsync(
        OrderInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    ) => Task.CompletedTask;
}

internal sealed class SecondTask : ITask<OrderInput>
{
    public static string TaskName => "generated.second";

    public Task ExecuteAsync(
        OrderInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    ) => Task.CompletedTask;
}

public static class Host
{
    public sealed class NestedTask : ITask<OrderInput>
    {
        public static string TaskName => "generated.nested";

        public Task ExecuteAsync(
            OrderInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        ) => Task.CompletedTask;
    }
}

/// <summary>
/// A task with a result, so the generated invoker's result path is exercised.
/// </summary>
public sealed class EchoTask : ITask<OrderInput, OrderInput>
{
    /// <summary>Gets the name the task routes by.</summary>
    public static string TaskName => "generated.echo";

    /// <inheritdoc />
    public Task<OrderInput> ExecuteAsync(
        OrderInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    ) => Task.FromResult(input);
}

/// <summary>
/// A task that implements only the bare <see cref="ITask"/> and declares the inputless
/// <c>ExecuteAsync(ITaskContext, CancellationToken)</c> convention.
/// </summary>
public sealed class NoInputTask : ITask
{
    /// <summary>Gets the name the task routes by.</summary>
    public static string TaskName => "generated.noinput";

    /// <summary>Executes the task without input.</summary>
#pragma warning disable CA1822 // Mark members as static - the convention is an instance method
    public Task<OrderInput> ExecuteAsync(
        ITaskContext context,
        CancellationToken cancellationToken = default
    ) => Task.FromResult(new OrderInput { Value = 42 });
#pragma warning restore CA1822
}

public sealed class OrderInput
{
    public int Value { get; init; }
}
