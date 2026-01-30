using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace DotCelery.Analyzers;

/// <summary>
/// Analyzer that checks if task input/output types are serializable (have parameterless constructors).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SerializationAnalyzer : DiagnosticAnalyzer
{
    private static readonly ImmutableHashSet<string> MutableCollectionTypes = ImmutableHashSet.Create(
        "System.Collections.Generic.List`1",
        "System.Collections.Generic.Dictionary`2",
        "System.Collections.Generic.HashSet`1",
        "System.Collections.ArrayList",
        "System.Collections.Hashtable");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(
            DiagnosticDescriptors.TypeMustBeSerializable,
            DiagnosticDescriptors.AvoidMutableCollections);

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

            compilationContext.RegisterSymbolAction(symbolContext =>
            {
                var namedType = (INamedTypeSymbol)symbolContext.Symbol;

                // Check if type implements ITask<TInput> or ITask<TInput, TOutput>
                foreach (var iface in namedType.AllInterfaces)
                {
                    if (iface.OriginalDefinition == null || !iface.OriginalDefinition.AllInterfaces.Any(i => SymbolEqualityComparer.Default.Equals(i, iTaskSymbol)))
                    {
                        continue;
                    }

                    // Check type arguments
                    foreach (var typeArg in iface.TypeArguments)
                    {
                        if (typeArg is INamedTypeSymbol typeArgSymbol)
                        {
                            // Determine if this is input or output
                            var paramType = iface.TypeArguments.IndexOf(typeArg) == 0 ? "input" : "output";

                            // Check for parameterless constructor (for classes)
                            if (typeArgSymbol.TypeKind == TypeKind.Class &&
                                !typeArgSymbol.IsRecord &&
                                !HasParameterlessConstructor(typeArgSymbol))
                            {
                                var diagnostic = Diagnostic.Create(
                                    DiagnosticDescriptors.TypeMustBeSerializable,
                                    namedType.Locations[0],
                                    typeArgSymbol.Name,
                                    paramType);
                                symbolContext.ReportDiagnostic(diagnostic);
                            }

                            // Check for mutable collections in properties
                            CheckForMutableCollections(typeArgSymbol, symbolContext);
                        }
                    }
                }
            }, SymbolKind.NamedType);
        });
    }

    private static bool HasParameterlessConstructor(INamedTypeSymbol typeSymbol)
    {
        // Records always have a primary constructor, but also support init/with patterns
        if (typeSymbol.IsRecord)
        {
            return true;
        }

        // Check for implicit parameterless constructor
        if (!typeSymbol.Constructors.Any())
        {
            return true;
        }

        // Check for explicit parameterless constructor
        return typeSymbol.Constructors.Any(c => c.Parameters.Length == 0 && c.DeclaredAccessibility == Accessibility.Public);
    }

    private static void CheckForMutableCollections(INamedTypeSymbol typeSymbol, SymbolAnalysisContext context)
    {
        foreach (var member in typeSymbol.GetMembers())
        {
            if (member is IPropertySymbol property && property.DeclaredAccessibility == Accessibility.Public)
            {
                var propertyType = property.Type;
                if (propertyType is INamedTypeSymbol namedPropertyType)
                {
                    var fullName = namedPropertyType.OriginalDefinition?.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    if (fullName != null && MutableCollectionTypes.Contains(fullName))
                    {
                        var diagnostic = Diagnostic.Create(
                            DiagnosticDescriptors.AvoidMutableCollections,
                            property.Locations[0],
                            typeSymbol.Name,
                            property.Name);
                        context.ReportDiagnostic(diagnostic);
                    }
                }
            }
        }
    }
}
