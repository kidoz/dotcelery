using System.Collections.Immutable;
using System.Linq;
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
            DiagnosticDescriptors.TaskMustBeSealed);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(compilationContext =>
        {
            var iTaskSymbol = compilationContext.Compilation.GetTypeByMetadataName("DotCelery.Core.Abstractions.ITask");
            if (iTaskSymbol == null)
            {
                // DotCelery.Core not referenced, skip analysis
                return;
            }

            var taskNames = new System.Collections.Concurrent.ConcurrentDictionary<string, INamedTypeSymbol>();

            compilationContext.RegisterSymbolAction(symbolContext =>
            {
                var namedType = (INamedTypeSymbol)symbolContext.Symbol;

                // Check if type implements ITask
                if (!namedType.AllInterfaces.Any(i => SymbolEqualityComparer.Default.Equals(i.OriginalDefinition, iTaskSymbol) ||
                                                      i.OriginalDefinition?.AllInterfaces.Any(ii => SymbolEqualityComparer.Default.Equals(ii, iTaskSymbol)) == true))
                {
                    return;
                }

                // Skip abstract classes
                if (namedType.IsAbstract)
                {
                    return;
                }

                // Check if task is sealed
                if (!namedType.IsSealed)
                {
                    var diagnostic = Diagnostic.Create(
                        DiagnosticDescriptors.TaskMustBeSealed,
                        namedType.Locations[0],
                        namedType.Name);
                    symbolContext.ReportDiagnostic(diagnostic);
                }

                // Find TaskName property
                var taskNameProperty = namedType.GetMembers("TaskName")
                    .OfType<IPropertySymbol>()
                    .FirstOrDefault(p => p.IsStatic);

                if (taskNameProperty == null)
                {
                    // TaskName not found - compiler will catch this
                    return;
                }

                // Get TaskName value from syntax
                var syntaxReference = taskNameProperty.DeclaringSyntaxReferences.FirstOrDefault();
                if (syntaxReference == null)
                {
                    return;
                }

                var syntax = syntaxReference.GetSyntax();
                if (syntax is PropertyDeclarationSyntax propertyDecl)
                {
                    var taskNameValue = ExtractTaskNameValue(propertyDecl);

                    // Check if TaskName is empty or null
                    if (taskNameValue == null || string.IsNullOrWhiteSpace(taskNameValue))
                    {
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.TaskNameCannotBeEmpty,
                            propertyDecl.GetLocation(),
                            namedType.Name);
                        symbolContext.ReportDiagnostic(diagnostic);
                        return;
                    }

                    // Check for duplicate TaskName
                    if (!taskNames.TryAdd(taskNameValue, namedType))
                    {
                        var existingTask = taskNames[taskNameValue];
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.DuplicateTaskName,
                            propertyDecl.GetLocation(),
                            namedType.Name,
                            taskNameValue);
                        symbolContext.ReportDiagnostic(diagnostic);
                    }
                }
            }, SymbolKind.NamedType);
        });
    }

    private static string? ExtractTaskNameValue(PropertyDeclarationSyntax propertyDecl)
    {
        // Handle: public static string TaskName => "value";
        if (propertyDecl.ExpressionBody?.Expression is LiteralExpressionSyntax literal &&
            literal.IsKind(SyntaxKind.StringLiteralExpression))
        {
            return literal.Token.ValueText;
        }

        // Handle: public static string TaskName { get => "value"; }
        var getter = propertyDecl.AccessorList?.Accessors
            .FirstOrDefault(a => a.IsKind(SyntaxKind.GetAccessorDeclaration));

        if (getter?.ExpressionBody?.Expression is LiteralExpressionSyntax getterLiteral &&
            getterLiteral.IsKind(SyntaxKind.StringLiteralExpression))
        {
            return getterLiteral.Token.ValueText;
        }

        // Handle: public static string TaskName { get { return "value"; } }
        if (getter?.Body != null)
        {
            var returnStatement = getter.Body.Statements
                .OfType<ReturnStatementSyntax>()
                .FirstOrDefault();

            if (returnStatement?.Expression is LiteralExpressionSyntax returnLiteral &&
                returnLiteral.IsKind(SyntaxKind.StringLiteralExpression))
            {
                return returnLiteral.Token.ValueText;
            }
        }

        return null;
    }
}
