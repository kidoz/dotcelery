using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace DotCelery.Analyzers.Tests.Verifiers;

/// <summary>
/// Helper for generator testing with DotCelery.Core references: runs the generator, and returns
/// the generated sources together with the compiler errors of the input and generated code.
/// </summary>
public static class CSharpGeneratorVerifier<TGenerator>
    where TGenerator : IIncrementalGenerator, new()
{
    /// <summary>
    /// Runs the generator over the source and compiles the result.
    /// </summary>
    /// <param name="source">The source to compile.</param>
    /// <returns>The generated sources and the compiler errors.</returns>
    public static GeneratorRun Run(string source)
    {
        var parseOptions = new CSharpParseOptions(LanguageVersion.Preview);
        var syntaxTree = CSharpSyntaxTree.ParseText(source, parseOptions);

        var references = new List<MetadataReference>
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Console).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IEnumerable<>).Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Collections").Location),
            MetadataReference.CreateFromFile(
                typeof(DotCelery.Core.Abstractions.ITask).Assembly.Location
            ),
            // The generated registration calls the builder, which stays in Core while the task
            // contracts live in Abstractions.
            MetadataReference.CreateFromFile(
                typeof(DotCelery.Core.Extensions.DotCeleryBuilder).Assembly.Location
            ),
        };

        var compilation = CSharpCompilation.Create(
            "TestAssembly",
            new[] { syntaxTree },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
        );

        // The generated trees inherit the driver's parse options, which must match the input's
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            new[] { new TGenerator().AsSourceGenerator() },
            parseOptions: parseOptions
        );
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var updated, out _);

        var sources = driver
            .GetRunResult()
            .Results.SelectMany(result => result.GeneratedSources)
            .Select(generated => generated.SourceText.ToString())
            .ToArray();
        var errors = updated
            .GetDiagnostics()
            .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();

        return new GeneratorRun(sources, errors);
    }
}

/// <summary>
/// The generated sources of a generator run, and the compiler errors of the compilation with
/// them.
/// </summary>
/// <param name="Sources">The generated sources.</param>
/// <param name="Errors">The compiler errors.</param>
public sealed record GeneratorRun(IReadOnlyList<string> Sources, IReadOnlyList<Diagnostic> Errors);
