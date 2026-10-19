using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace DotCelery.Analyzers;

/// <summary>
/// Generates a task registration for the tasks defined in the compilation, so the worker's
/// registry is built from typed accesses to each task's static name and the tasks are invoked
/// through typed adapters, instead of reflection and expression compilation.
/// </summary>
/// <remarks>
/// The generated class is internal to the assembly that defines the tasks, in the
/// <c>DotCelery.Generated</c> namespace: a library that ships tasks calls it from its own
/// registration method, and an application calls it from its composition root.
/// </remarks>
[Generator(LanguageNames.CSharp)]
public sealed class TaskRegistrationGenerator : IIncrementalGenerator
{
    /// <summary>The namespace of the generated registration class.</summary>
    public const string GeneratedNamespace = "DotCelery.Generated";

    /// <summary>The name of the generated registration class.</summary>
    public const string GeneratedClassName = "DotCeleryTasks";

    /// <summary>The name of the generated registration method.</summary>
    public const string RegisterMethodName = "RegisterTasks";

    private const string TaskInterfaceNamespace = "DotCelery.Core.Abstractions";
    private const string TaskInterfaceName = "ITask";
    private const string ContextInterfaceName = "DotCelery.Core.Abstractions.ITaskContext";
    private const string CancellationTokenName = "System.Threading.CancellationToken";

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var tasks = context
            .SyntaxProvider.CreateSyntaxProvider(
                predicate: static (node, _) => node is ClassDeclarationSyntax,
                transform: static (nodeContext, cancellationToken) =>
                    GetTask(nodeContext, cancellationToken)
            )
            .Where(static task => task is not null)
            .Select(static (task, _) => task!.Value)
            .Collect();

        context.RegisterSourceOutput(tasks, static (production, tasks) => Emit(production, tasks));
    }

    private static TaskModel? GetTask(
        GeneratorSyntaxContext context,
        CancellationToken cancellationToken
    )
    {
        var declaration = (ClassDeclarationSyntax)context.Node;
        if (
            context.SemanticModel.GetDeclaredSymbol(declaration, cancellationToken)
            is not INamedTypeSymbol taskType
        )
        {
            return null;
        }

        if (!IsTask(taskType) || taskType.IsAbstract || taskType.IsGenericType)
        {
            // An abstract type is never instantiated and an open generic cannot be registered
            return null;
        }

        if (!IsAccessible(taskType))
        {
            // A private type cannot be referenced from the generated class
            return null;
        }

        var typeName = taskType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var adapterName = MakeAdapterName(taskType) + "_DotCeleryInvoker";
        var taskInterfaces = GetGenericTaskInterfaces(taskType);

        if (taskInterfaces.Count > 1)
        {
            // The registry picks one of them, and the adapter cannot know which, so the task keeps
            // the compiled invoker instead
            return new TaskModel(typeName, AdapterName: null, InputTypeName: null);
        }

        if (taskInterfaces.Count == 1)
        {
            var taskInterface = taskInterfaces[0];
            return new TaskModel(
                typeName,
                adapterName,
                InputTypeName: taskInterface
                    .TypeArguments[0]
                    .ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                InterfaceName: taskInterface.ToDisplayString(
                    SymbolDisplayFormat.FullyQualifiedFormat
                ),
                ReturnsResult: taskInterface.Arity == 2
            );
        }

        // A task may implement only the bare ITask and declare the inputless ExecuteAsync
        var method = FindInputlessExecuteAsync(taskType);
        if (method is null)
        {
            return new TaskModel(typeName, AdapterName: null, InputTypeName: null);
        }

        return new TaskModel(
            typeName,
            adapterName,
            InputTypeName: null,
            InterfaceName: null,
            ReturnsResult: method.ReturnType is INamedTypeSymbol { Arity: 1 }
        );
    }

    // Every task interface extends ITask, so finding it covers all of them
    private static bool IsTask(INamedTypeSymbol type)
    {
        foreach (var implemented in type.AllInterfaces)
        {
            var definition = implemented.OriginalDefinition;
            if (
                definition.Name == TaskInterfaceName
                && definition.ContainingNamespace?.ToDisplayString() == TaskInterfaceNamespace
            )
            {
                return true;
            }
        }

        return false;
    }

    private static List<INamedTypeSymbol> GetGenericTaskInterfaces(INamedTypeSymbol type) =>
        [
            .. type
                .AllInterfaces.Where(implemented =>
                    implemented.OriginalDefinition.Name == TaskInterfaceName
                    && implemented.OriginalDefinition.Arity is 1 or 2
                    && implemented.OriginalDefinition.ContainingNamespace?.ToDisplayString()
                        == TaskInterfaceNamespace
                )
                .Distinct(SymbolEqualityComparer.Default)
                .OfType<INamedTypeSymbol>(),
        ];

    private static IMethodSymbol? FindInputlessExecuteAsync(INamedTypeSymbol type) =>
        type.GetMembers("ExecuteAsync")
            .OfType<IMethodSymbol>()
            .FirstOrDefault(method =>
                !method.IsStatic
                && method.DeclaredAccessibility == Accessibility.Public
                && method.Parameters.Length == 2
                && method.Parameters[0].Type.ToDisplayString() == ContextInterfaceName
                && method.Parameters[1].Type.ToDisplayString() == CancellationTokenName
            );

    private static bool IsAccessible(INamedTypeSymbol type)
    {
        for (var current = type; current is not null; current = current.ContainingType)
        {
            if (
                current.IsFileLocal
                || current.DeclaredAccessibility
                    is Accessibility.Private
                        or Accessibility.Protected
                        or Accessibility.ProtectedAndInternal
            )
            {
                return false;
            }
        }

        return true;
    }

    private static string MakeAdapterName(INamedTypeSymbol type) =>
        type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
            .Replace("global::", string.Empty)
            .Replace('.', '_');

    private static void Emit(SourceProductionContext context, ImmutableArray<TaskModel> tasks)
    {
        var ordered = tasks.OrderBy(task => task.TypeName, StringComparer.Ordinal).ToList();

        if (ordered.Count == 0)
        {
            return;
        }

        var source = new StringBuilder();
        source.AppendLine("// <auto-generated/>");
        source.AppendLine("#nullable enable");
        source.AppendLine();
        source.AppendLine($"namespace {GeneratedNamespace};");
        source.AppendLine();

        foreach (var task in ordered.Where(task => task.AdapterName is not null))
        {
            AppendAdapter(source, task);
        }

        AppendRegistration(source, ordered);

        context.AddSource(
            $"{GeneratedClassName}.g.cs",
            SourceText.From(source.ToString(), Encoding.UTF8)
        );
    }

    private static void AppendAdapter(StringBuilder source, TaskModel task)
    {
        source.AppendLine("/// <summary>");
        source.AppendLine(
            $"/// Invokes {task.TypeName} through its typed adapter, without expression compilation."
        );
        source.AppendLine("/// </summary>");
        source.AppendLine(
            $"internal sealed class {task.AdapterName} : global::DotCelery.Core.Abstractions.ITaskInvoker"
        );
        source.AppendLine("{");
        source.AppendLine(
            "    public async global::System.Threading.Tasks.Task<object?> InvokeAsync("
        );
        source.AppendLine("        object task,");
        source.AppendLine("        object? input,");
        source.AppendLine("        global::DotCelery.Core.Abstractions.ITaskContext context,");
        source.AppendLine(
            "        global::System.Threading.CancellationToken cancellationToken = default)"
        );
        source.AppendLine("    {");

        var target = task.InterfaceName ?? task.TypeName;
        var arguments = task.InputTypeName is null
            ? "context, cancellationToken"
            : $"({task.InputTypeName})input!, context, cancellationToken";
        var call = $"(({target})task).ExecuteAsync({arguments})";

        if (task.ReturnsResult)
        {
            source.AppendLine($"        return await {call}.ConfigureAwait(false);");
        }
        else
        {
            source.AppendLine($"        await {call}.ConfigureAwait(false);");
            source.AppendLine("        return null;");
        }

        source.AppendLine("    }");
        source.AppendLine("}");
        source.AppendLine();
    }

    private static void AppendRegistration(StringBuilder source, List<TaskModel> tasks)
    {
        source.AppendLine("/// <summary>");
        source.AppendLine(
            "/// Registers the tasks of this assembly by the names they route by, without reflection."
        );
        source.AppendLine("/// </summary>");
        source.AppendLine($"internal static class {GeneratedClassName}");
        source.AppendLine("{");
        source.AppendLine("    /// <summary>");
        source.AppendLine("    /// Registers the tasks of this assembly.");
        source.AppendLine("    /// </summary>");
        source.AppendLine("    /// <param name=\"builder\">The builder.</param>");
        source.AppendLine("    /// <returns>The builder.</returns>");
        source.AppendLine(
            $"    public static global::DotCelery.Core.Extensions.DotCeleryBuilder {RegisterMethodName}("
        );
        source.AppendLine(
            "        this global::DotCelery.Core.Extensions.DotCeleryBuilder builder)"
        );
        source.AppendLine("    {");

        foreach (var task in tasks)
        {
            var invoker = task.AdapterName is null ? string.Empty : $", new {task.AdapterName}()";
            source.AppendLine(
                $"        builder.AddTask<{task.TypeName}>({task.TypeName}.TaskName{invoker});"
            );
        }

        source.AppendLine("        return builder;");
        source.AppendLine("    }");
        source.AppendLine("}");
    }

    /// <summary>
    /// A task of the compilation: its routing name access, and the adapter that invokes it.
    /// </summary>
    private readonly record struct TaskModel(
        string TypeName,
        string? AdapterName,
        string? InputTypeName,
        string? InterfaceName = null,
        bool ReturnsResult = false
    );
}
