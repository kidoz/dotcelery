using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace DotCelery.Analyzers;

/// <summary>
/// Analyzer that validates ITask implementations have unique, non-empty TaskName properties.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TaskNameAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(
            DiagnosticDescriptors.TaskNameCannotBeEmpty,
            DiagnosticDescriptors.DuplicateTaskName,
            DiagnosticDescriptors.TaskMustBeSealed
        );

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(compilationContext =>
        {
            var iTaskSymbol = compilationContext.Compilation.GetTypeByMetadataName(
                "DotCelery.Core.Abstractions.ITask"
            );
            if (iTaskSymbol == null)
            {
                // DotCelery.Core not referenced, skip analysis
                return;
            }

            var taskNames = new System.Collections.Concurrent.ConcurrentDictionary<
                string,
                System.Collections.Concurrent.ConcurrentBag<(
                    INamedTypeSymbol Type,
                    Location Location
                )>
            >();

            compilationContext.RegisterSymbolAction(
                symbolContext =>
                {
                    var namedType = (INamedTypeSymbol)symbolContext.Symbol;

                    // Skip types that do not implement ITask, and abstract base classes
                    if (!IsTaskType(namedType, iTaskSymbol) || namedType.IsAbstract)
                    {
                        return;
                    }

                    // Check if task is sealed
                    if (!namedType.IsSealed)
                    {
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.TaskMustBeSealed,
                            namedType.Locations[0],
                            namedType.Name
                        );
                        symbolContext.ReportDiagnostic(diagnostic);
                    }
                },
                SymbolKind.NamedType
            );

            // The TaskName value is evaluated through the semantic model, which a symbol action
            // does not provide; it resolves literals, constant references, and folded constant
            // expressions alike
            compilationContext.RegisterSyntaxNodeAction(
                syntaxContext =>
                {
                    var propertyDecl = (PropertyDeclarationSyntax)syntaxContext.Node;

                    if (
                        syntaxContext.SemanticModel.GetDeclaredSymbol(propertyDecl)
                            is not IPropertySymbol property
                        || !property.IsStatic
                        || property.Name != "TaskName"
                    )
                    {
                        return;
                    }

                    var namedType = property.ContainingType;
                    if (
                        namedType is null
                        || !IsTaskType(namedType, iTaskSymbol)
                        || namedType.IsAbstract
                    )
                    {
                        return;
                    }

                    // A name that is not a compile-time constant cannot be evaluated here, so
                    // only a constant is validated and tracked
                    if (
                        !TryGetTaskNameValue(
                            propertyDecl,
                            syntaxContext.SemanticModel,
                            out var taskNameValue
                        )
                    )
                    {
                        return;
                    }

                    // Check if TaskName is empty
                    if (string.IsNullOrWhiteSpace(taskNameValue))
                    {
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.TaskNameCannotBeEmpty,
                            propertyDecl.Identifier.GetLocation(),
                            namedType.Name
                        );
                        syntaxContext.ReportDiagnostic(diagnostic);
                        return;
                    }

                    taskNames
                        .GetOrAdd(
                            taskNameValue,
                            _ => new System.Collections.Concurrent.ConcurrentBag<(
                                INamedTypeSymbol Type,
                                Location Location
                            )>()
                        )
                        .Add((namedType, propertyDecl.Identifier.GetLocation()));
                },
                SyntaxKind.PropertyDeclaration
            );

            compilationContext.RegisterCompilationEndAction(endContext =>
            {
                foreach (var duplicateGroup in taskNames)
                {
                    var orderedTasks = duplicateGroup
                        .Value.OrderBy(item => item.Location.SourceSpan.Start)
                        .ToList();
                    if (orderedTasks.Count <= 1)
                    {
                        continue;
                    }

                    foreach (var duplicateTask in orderedTasks.Skip(1))
                    {
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.DuplicateTaskName,
                            duplicateTask.Location,
                            duplicateTask.Type.Name,
                            duplicateGroup.Key
                        );
                        endContext.ReportDiagnostic(diagnostic);
                    }
                }
            });
        });
    }

    /// <summary>
    /// Gets whether a type implements <c>ITask</c>, directly or through an interface it implements.
    /// </summary>
    private static bool IsTaskType(INamedTypeSymbol namedType, INamedTypeSymbol iTaskSymbol) =>
        namedType.AllInterfaces.Any(i =>
            SymbolEqualityComparer.Default.Equals(i.OriginalDefinition, iTaskSymbol)
            || i.OriginalDefinition?.AllInterfaces.Any(ii =>
                SymbolEqualityComparer.Default.Equals(ii, iTaskSymbol)
            ) == true
        );

    /// <summary>
    /// Gets the value of a TaskName property when it is a compile-time constant, which includes
    /// string literals, references to constants, and constant expressions the compiler folds.
    /// </summary>
    private static bool TryGetTaskNameValue(
        PropertyDeclarationSyntax propertyDecl,
        SemanticModel semanticModel,
        out string value
    )
    {
        value = string.Empty;

        var expression = GetValueExpression(propertyDecl);
        if (expression is null)
        {
            return false;
        }

        var constant = semanticModel.GetConstantValue(expression);
        if (!constant.HasValue)
        {
            // A name computed at run time cannot be evaluated here
            return false;
        }

        value = constant.Value as string ?? string.Empty;
        return true;
    }

    private static ExpressionSyntax? GetValueExpression(PropertyDeclarationSyntax propertyDecl)
    {
        // Handle: public static string TaskName => "value";
        if (propertyDecl.ExpressionBody is not null)
        {
            return propertyDecl.ExpressionBody.Expression;
        }

        var getter = propertyDecl.AccessorList?.Accessors.FirstOrDefault(a =>
            a.IsKind(SyntaxKind.GetAccessorDeclaration)
        );

        // Handle: public static string TaskName { get => "value"; }
        if (getter?.ExpressionBody is not null)
        {
            return getter.ExpressionBody.Expression;
        }

        // Handle: public static string TaskName { get { return "value"; } }
        var returnStatement = getter
            ?.Body?.Statements.OfType<ReturnStatementSyntax>()
            .FirstOrDefault();

        return returnStatement?.Expression;
    }
}
