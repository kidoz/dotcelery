using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace DotCelery.Analyzers;

/// <summary>
/// Analyzer that detects blocking calls (Task.Wait, Task.Result, .GetAwaiter().GetResult()) in ITask.ExecuteAsync implementations.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class BlockingCallAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(DiagnosticDescriptors.AvoidBlockingCallsInTasks);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(compilationContext =>
        {
            var iTaskSymbol = compilationContext.Compilation.GetTypeByMetadataName("DotCelery.Core.Abstractions.ITask");
            if (iTaskSymbol == null)
            {
                return;
            }

            compilationContext.RegisterSyntaxNodeAction(nodeContext =>
            {
                // Check if we're inside an ExecuteAsync method of a task class
                if (!IsInsideTaskExecuteAsync(nodeContext.Node, nodeContext.SemanticModel, iTaskSymbol))
                {
                    return;
                }

                // Check for Task.Wait()
                if (nodeContext.Node is InvocationExpressionSyntax invocation)
                {
                    if (IsBlockingCall(invocation, nodeContext.SemanticModel, out var blockingMethod))
                    {
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.AvoidBlockingCallsInTasks,
                            invocation.GetLocation(),
                            blockingMethod);
                        nodeContext.ReportDiagnostic(diagnostic);
                    }
                }

                // Check for Task.Result
                if (nodeContext.Node is MemberAccessExpressionSyntax memberAccess)
                {
                    if (IsBlockingPropertyAccess(memberAccess, nodeContext.SemanticModel, out var blockingProperty))
                    {
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.AvoidBlockingCallsInTasks,
                            memberAccess.GetLocation(),
                            blockingProperty);
                        nodeContext.ReportDiagnostic(diagnostic);
                    }
                }
            }, SyntaxKind.InvocationExpression, SyntaxKind.SimpleMemberAccessExpression);
        });
    }

    private static bool IsInsideTaskExecuteAsync(SyntaxNode node, SemanticModel semanticModel, INamedTypeSymbol iTaskSymbol)
    {
        var method = node.FirstAncestorOrSelf<MethodDeclarationSyntax>();
        if (method == null || method.Identifier.Text != "ExecuteAsync")
        {
            return false;
        }

        var methodSymbol = semanticModel.GetDeclaredSymbol(method);
        if (methodSymbol == null)
        {
            return false;
        }

        var containingType = methodSymbol.ContainingType;
        return containingType.AllInterfaces.Any(i =>
            SymbolEqualityComparer.Default.Equals(i.OriginalDefinition, iTaskSymbol) ||
            i.OriginalDefinition?.AllInterfaces.Any(ii => SymbolEqualityComparer.Default.Equals(ii, iTaskSymbol)) == true);
    }

    private static bool IsBlockingCall(InvocationExpressionSyntax invocation, SemanticModel semanticModel, out string blockingMethod)
    {
        blockingMethod = string.Empty;

        var symbolInfo = semanticModel.GetSymbolInfo(invocation);
        if (symbolInfo.Symbol is not IMethodSymbol methodSymbol)
        {
            return false;
        }

        var containingType = methodSymbol.ContainingType;
        if (containingType == null)
        {
            return false;
        }

        // Check for Task.Wait()
        if (methodSymbol.Name == "Wait" &&
            (containingType.Name == "Task" || containingType.OriginalDefinition?.Name == "Task") &&
            containingType.ContainingNamespace?.ToString() == "System.Threading.Tasks")
        {
            blockingMethod = "Task.Wait()";
            return true;
        }

        // Check for .GetAwaiter().GetResult()
        if (methodSymbol.Name == "GetResult" &&
            invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
            memberAccess.Expression is InvocationExpressionSyntax getAwaiterInvocation)
        {
            var getAwaiterSymbol = semanticModel.GetSymbolInfo(getAwaiterInvocation).Symbol as IMethodSymbol;
            if (getAwaiterSymbol?.Name == "GetAwaiter")
            {
                blockingMethod = ".GetAwaiter().GetResult()";
                return true;
            }
        }

        return false;
    }

    private static bool IsBlockingPropertyAccess(MemberAccessExpressionSyntax memberAccess, SemanticModel semanticModel, out string blockingProperty)
    {
        blockingProperty = string.Empty;

        var symbolInfo = semanticModel.GetSymbolInfo(memberAccess);
        if (symbolInfo.Symbol is not IPropertySymbol propertySymbol)
        {
            return false;
        }

        var containingType = propertySymbol.ContainingType;
        if (containingType == null)
        {
            return false;
        }

        // Check for Task.Result
        if (propertySymbol.Name == "Result" &&
            (containingType.Name == "Task" || containingType.OriginalDefinition?.Name == "Task") &&
            containingType.ContainingNamespace?.ToString() == "System.Threading.Tasks")
        {
            blockingProperty = "Task.Result";
            return true;
        }

        return false;
    }
}
