using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace DotCelery.Analyzers;

/// <summary>
/// Analyzer that validates DotCelery attribute usage (TimeLimitAttribute, RouteAttribute, PreventOverlappingAttribute).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class AttributeValidationAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(
            DiagnosticDescriptors.InvalidTimeLimitConfiguration,
            DiagnosticDescriptors.InvalidRouteAttribute,
            DiagnosticDescriptors.InvalidPreventOverlappingConfiguration);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(compilationContext =>
        {
            var timeLimitAttributeSymbol = compilationContext.Compilation.GetTypeByMetadataName("DotCelery.Core.Attributes.TimeLimitAttribute");
            var routeAttributeSymbol = compilationContext.Compilation.GetTypeByMetadataName("DotCelery.Core.Routing.RouteAttribute");
            var preventOverlappingAttributeSymbol = compilationContext.Compilation.GetTypeByMetadataName("DotCelery.Core.Attributes.PreventOverlappingAttribute");

            if (timeLimitAttributeSymbol == null && routeAttributeSymbol == null && preventOverlappingAttributeSymbol == null)
            {
                return;
            }

            compilationContext.RegisterSymbolAction(symbolContext =>
            {
                var namedType = (INamedTypeSymbol)symbolContext.Symbol;

                foreach (var attribute in namedType.GetAttributes())
                {
                    // Validate TimeLimitAttribute
                    if (timeLimitAttributeSymbol != null &&
                        SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, timeLimitAttributeSymbol))
                    {
                        ValidateTimeLimitAttribute(attribute, symbolContext);
                    }

                    // Validate RouteAttribute
                    if (routeAttributeSymbol != null &&
                        SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, routeAttributeSymbol))
                    {
                        ValidateRouteAttribute(attribute, symbolContext);
                    }

                    // Validate PreventOverlappingAttribute
                    if (preventOverlappingAttributeSymbol != null &&
                        SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, preventOverlappingAttributeSymbol))
                    {
                        ValidatePreventOverlappingAttribute(attribute, symbolContext);
                    }
                }
            }, SymbolKind.NamedType);
        });
    }

    private static void ValidateTimeLimitAttribute(AttributeData attribute, SymbolAnalysisContext context)
    {
        int? softLimit = null;
        int? hardLimit = null;

        foreach (var namedArg in attribute.NamedArguments)
        {
            if (namedArg.Key == "SoftLimitSeconds" && namedArg.Value.Value is int soft)
            {
                softLimit = soft;
            }
            else if (namedArg.Key == "HardLimitSeconds" && namedArg.Value.Value is int hard)
            {
                hardLimit = hard;
            }
        }

        // Validate both limits are positive
        if (softLimit.HasValue && softLimit.Value <= 0)
        {
            var diagnostic = Diagnostic.Create(
                DiagnosticDescriptors.InvalidTimeLimitConfiguration,
                attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? context.Symbol.Locations[0],
                "SoftLimitSeconds must be greater than 0");
            context.ReportDiagnostic(diagnostic);
        }

        if (hardLimit.HasValue && hardLimit.Value <= 0)
        {
            var diagnostic = Diagnostic.Create(
                DiagnosticDescriptors.InvalidTimeLimitConfiguration,
                attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? context.Symbol.Locations[0],
                "HardLimitSeconds must be greater than 0");
            context.ReportDiagnostic(diagnostic);
        }

        // Validate soft < hard
        if (softLimit.HasValue && hardLimit.HasValue && softLimit.Value >= hardLimit.Value)
        {
            var diagnostic = Diagnostic.Create(
                DiagnosticDescriptors.InvalidTimeLimitConfiguration,
                attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? context.Symbol.Locations[0],
                "SoftLimitSeconds must be less than HardLimitSeconds");
            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void ValidateRouteAttribute(AttributeData attribute, SymbolAnalysisContext context)
    {
        // RouteAttribute has a required constructor parameter
        if (attribute.ConstructorArguments.Length > 0)
        {
            var queueArg = attribute.ConstructorArguments[0];
            if (queueArg.Value is string queue && string.IsNullOrWhiteSpace(queue))
            {
                var diagnostic = Diagnostic.Create(
                    DiagnosticDescriptors.InvalidRouteAttribute,
                    attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? context.Symbol.Locations[0]);
                context.ReportDiagnostic(diagnostic);
            }
        }
    }

    private static void ValidatePreventOverlappingAttribute(AttributeData attribute, SymbolAnalysisContext context)
    {
        int? timeoutSeconds = null;
        bool keyByInput = false;
        string? keyProperty = null;

        foreach (var namedArg in attribute.NamedArguments)
        {
            if (namedArg.Key == "TimeoutSeconds" && namedArg.Value.Value is int timeout)
            {
                timeoutSeconds = timeout;
            }
            else if (namedArg.Key == "KeyByInput" && namedArg.Value.Value is bool keyBy)
            {
                keyByInput = keyBy;
            }
            else if (namedArg.Key == "KeyProperty" && namedArg.Value.Value is string keyProp)
            {
                keyProperty = keyProp;
            }
        }

        // Validate timeout is positive
        if (timeoutSeconds.HasValue && timeoutSeconds.Value <= 0)
        {
            var diagnostic = Diagnostic.Create(
                DiagnosticDescriptors.InvalidPreventOverlappingConfiguration,
                attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? context.Symbol.Locations[0],
                "TimeoutSeconds must be greater than 0");
            context.ReportDiagnostic(diagnostic);
        }

        // Validate KeyProperty only makes sense when KeyByInput is true
        if (!string.IsNullOrEmpty(keyProperty) && !keyByInput)
        {
            var diagnostic = Diagnostic.Create(
                DiagnosticDescriptors.InvalidPreventOverlappingConfiguration,
                attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation() ?? context.Symbol.Locations[0],
                "KeyProperty can only be set when KeyByInput is true");
            context.ReportDiagnostic(diagnostic);
        }
    }
}
