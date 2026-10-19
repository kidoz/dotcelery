using DotCelery.Core.Abstractions;

namespace DotCelery.Core.Extensions;

/// <summary>
/// A task type registered on the <see cref="DotCeleryBuilder"/>, with the name it routes by.
/// </summary>
/// <param name="TaskType">The task type.</param>
/// <param name="TaskName">The name the task routes by.</param>
/// <param name="Invoker">The typed adapter that invokes the task, when one was generated.</param>
/// <param name="InputType">
/// The input type, when it is known without inspecting the task type; null lets the registry
/// resolve it.
/// </param>
/// <param name="OutputType">
/// The output type, when it is known without inspecting the task type; null lets the registry
/// resolve it.
/// </param>
public sealed record TaskTypeRegistration(
    Type TaskType,
    string TaskName,
    ITaskInvoker? Invoker = null,
    Type? InputType = null,
    Type? OutputType = null
);
