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

public sealed class OrderInput
{
    public int Value { get; init; }
}
