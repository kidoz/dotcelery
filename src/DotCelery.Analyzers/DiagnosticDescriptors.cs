using Microsoft.CodeAnalysis;

namespace DotCelery.Analyzers;

/// <summary>
/// Diagnostic descriptors for DotCelery analyzers.
/// </summary>
public static class DiagnosticDescriptors
{
    private const string Category = "DotCelery";

    // DCEL001-099: Task Definition Issues
    public static readonly DiagnosticDescriptor TaskNameCannotBeEmpty = new(
        id: "DCEL001",
        title: "Task name cannot be null, empty, or whitespace",
        messageFormat: "Task '{0}' has an invalid TaskName. TaskName must be a non-empty string.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "TaskName is used for routing and must be a valid identifier.");

    public static readonly DiagnosticDescriptor DuplicateTaskName = new(
        id: "DCEL002",
        title: "Duplicate task name detected",
        messageFormat: "Task '{0}' has a duplicate TaskName '{1}'. Each task must have a unique name.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Task names must be unique across the application for proper routing.");

    public static readonly DiagnosticDescriptor TaskMustBeSealed = new(
        id: "DCEL003",
        title: "Task classes should be sealed",
        messageFormat: "Task '{0}' should be sealed. Tasks are not designed for inheritance.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Task classes should be sealed to prevent inheritance issues.");

    // DCEL100-199: Async/Await Issues
    public static readonly DiagnosticDescriptor AvoidBlockingCallsInTasks = new(
        id: "DCEL100",
        title: "Avoid blocking calls in async task execution",
        messageFormat: "Blocking call '{0}' detected in task execution. Use async/await instead.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Blocking calls like Task.Wait(), Task.Result, or .GetAwaiter().GetResult() can cause deadlocks. Use await instead.");

    // DCEL200-299: Attribute Validation
    public static readonly DiagnosticDescriptor InvalidTimeLimitConfiguration = new(
        id: "DCEL200",
        title: "Invalid time limit configuration",
        messageFormat: "Invalid time limit configuration: {0}.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Time limits must be positive and SoftLimit must be less than HardLimit.");

    public static readonly DiagnosticDescriptor InvalidRouteAttribute = new(
        id: "DCEL201",
        title: "Invalid route queue name",
        messageFormat: "Route attribute queue name cannot be null, empty, or whitespace.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Queue names must be valid non-empty strings.");

    public static readonly DiagnosticDescriptor InvalidPreventOverlappingConfiguration = new(
        id: "DCEL202",
        title: "Invalid PreventOverlapping configuration",
        messageFormat: "Invalid PreventOverlapping configuration: {0}.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "PreventOverlapping attribute must be configured correctly.");

    // DCEL300-399: Serialization Issues
    public static readonly DiagnosticDescriptor TypeMustBeSerializable = new(
        id: "DCEL300",
        title: "Task input/output type must be serializable",
        messageFormat: "Type '{0}' used as task {1} must have a parameterless constructor for JSON serialization.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Task input and output types must be serializable. Consider adding a parameterless constructor or using records.");

    public static readonly DiagnosticDescriptor AvoidMutableCollections = new(
        id: "DCEL301",
        title: "Avoid mutable collections in task input/output",
        messageFormat: "Type '{0}' contains mutable collection property '{1}'. Consider using immutable collections.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Info,
        isEnabledByDefault: true,
        description: "Mutable collections can lead to race conditions in distributed scenarios.");

    // DCEL400-499: Usage Issues
    public static readonly DiagnosticDescriptor SendAsyncTypeMismatch = new(
        id: "DCEL400",
        title: "SendAsync type parameters do not match task definition",
        messageFormat: "SendAsync type parameters TInput='{0}' and TOutput='{1}' do not match task '{2}' definition.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "The input and output types specified in SendAsync must match the task's generic parameters.");

    public static readonly DiagnosticDescriptor MissingTaskRegistration = new(
        id: "DCEL401",
        title: "Task not registered in DI container",
        messageFormat: "Task '{0}' is used but may not be registered. Ensure AddTask<{0}>() is called in service configuration.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Info,
        isEnabledByDefault: true,
        description: "Tasks must be registered in the DI container to be executed by workers.");
}
