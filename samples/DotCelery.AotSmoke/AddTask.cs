using System.Text.Json.Serialization;
using DotCelery.Core.Abstractions;

namespace DotCelery.AotSmoke;

/// <summary>
/// Input of the smoke-test task.
/// </summary>
public sealed record AddInput
{
    public required int A { get; init; }

    public required int B { get; init; }
}

/// <summary>
/// Result of the smoke-test task.
/// </summary>
public sealed record AddOutput
{
    public required int Sum { get; init; }
}

/// <summary>
/// The smoke-test task, registered through the source generator rather than a reflective scan.
/// </summary>
public sealed class AddTask : ITask<AddInput, AddOutput>
{
    public static string TaskName => "aot.add";

    public Task<AddOutput> ExecuteAsync(
        AddInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default
    )
    {
        return Task.FromResult(new AddOutput { Sum = input.A + input.B });
    }
}

/// <summary>
/// Registers the payload types with a source-generated context, so the Native AOT build never
/// falls back to reflection-based serialization.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(AddInput))]
[JsonSerializable(typeof(AddOutput))]
internal sealed partial class SmokeJsonContext : JsonSerializerContext;
