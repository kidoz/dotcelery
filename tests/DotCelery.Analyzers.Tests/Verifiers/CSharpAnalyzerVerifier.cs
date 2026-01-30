using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text.RegularExpressions;

namespace DotCelery.Analyzers.Tests.Verifiers;

/// <summary>
/// Helper class for analyzer testing with DotCelery.Core references.
/// </summary>
public static partial class CSharpAnalyzerVerifier<TAnalyzer>
    where TAnalyzer : DiagnosticAnalyzer, new()
{
    public static DiagnosticDescriptor Diagnostic(DiagnosticDescriptor descriptor) =>
        descriptor;

    public static async Task VerifyAnalyzerAsync(string source, params DiagnosticDescriptor[] expectedDescriptors)
    {
        var (code, expectedLocations) = ExtractMarkedSpans(source);
        var compilation = CreateCompilation(code);
        var diagnostics = await GetDiagnosticsAsync(compilation);

        // Verify expected diagnostics
        if (expectedDescriptors.Length != expectedLocations.Count)
        {
            var diagMessages = string.Join(Environment.NewLine, diagnostics.Select(d => $"  - {d.Id}: {d.GetMessage()}"));
            throw new InvalidOperationException(
                $"Expected {expectedDescriptors.Length} diagnostic(s), but got {expectedLocations.Count} marked location(s) and {diagnostics.Length} diagnostic(s).{Environment.NewLine}Diagnostics:{Environment.NewLine}{diagMessages}");
        }

        for (var i = 0; i < expectedDescriptors.Length; i++)
        {
            var expected = expectedDescriptors[i];
            var actualDiagnostic = diagnostics.FirstOrDefault(d => d.Id == expected.Id);

            if (actualDiagnostic == null)
            {
                var actualIds = string.Join(", ", diagnostics.Select(d => d.Id));
                throw new InvalidOperationException(
                    $"Expected diagnostic '{expected.Id}' was not found. Actual diagnostics: {actualIds}");
            }

            // Verify location if marked
            if (i < expectedLocations.Count)
            {
                var expectedLocation = expectedLocations[i];
                var actualLocation = actualDiagnostic.Location.SourceSpan;

                if (expectedLocation != actualLocation)
                {
                    throw new InvalidOperationException(
                        $"Diagnostic '{expected.Id}' location mismatch. Expected: {expectedLocation}, Actual: {actualLocation}");
                }
            }
        }

        // Verify no unexpected diagnostics
        var unexpectedDiagnostics = diagnostics.Where(d => !expectedDescriptors.Any(ed => ed.Id == d.Id)).ToList();
        if (unexpectedDiagnostics.Any())
        {
            var messages = string.Join(Environment.NewLine, unexpectedDiagnostics.Select(d => $"  - {d.Id}: {d.GetMessage()}"));
            throw new InvalidOperationException($"Unexpected diagnostics found:{Environment.NewLine}{messages}");
        }
    }

    private static CSharpCompilation CreateCompilation(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Preview));

        var references = new List<MetadataReference>
        {
            // System references
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Console).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IEnumerable<>).Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Collections").Location),

            // DotCelery.Core reference
            MetadataReference.CreateFromFile(typeof(DotCelery.Core.Abstractions.ITask).Assembly.Location),
        };

        return CSharpCompilation.Create(
            "TestAssembly",
            new[] { syntaxTree },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
    }

    private static async Task<Diagnostic[]> GetDiagnosticsAsync(CSharpCompilation compilation)
    {
        var analyzers = ImmutableArray.Create<DiagnosticAnalyzer>(new TAnalyzer());

        var compilationWithAnalyzers = compilation.WithAnalyzers(analyzers);
        var allDiagnostics = await compilationWithAnalyzers.GetAllDiagnosticsAsync();

        return allDiagnostics
            .Where(d => d.Severity != DiagnosticSeverity.Hidden)
            .Where(d => !d.Id.StartsWith("CS", StringComparison.Ordinal))  // Filter out compiler errors
            .ToArray();
    }

    private static (string code, List<TextSpan> locations) ExtractMarkedSpans(string markedSource)
    {
        var code = markedSource;
        var locations = new List<TextSpan>();

        // Match {|#0:...|} patterns
        var regex = MarkerRegex();
        var matches = regex.Matches(markedSource);

        // Process matches in reverse order to preserve positions
        for (var i = matches.Count - 1; i >= 0; i--)
        {
            var match = matches[i];
            var index = int.Parse(match.Groups[1].Value);
            var content = match.Groups[2].Value;
            var start = match.Index;

            // Calculate the actual position after removing previous markers
            var adjustedStart = start - (matches.Count - 1 - i) * (match.Value.Length - content.Length);

            locations.Insert(0, new TextSpan(adjustedStart, content.Length));

            // Remove the marker, keeping only the content
            code = code.Substring(0, match.Index) + content + code.Substring(match.Index + match.Length);
        }

        return (code, locations);
    }

    [GeneratedRegex(@"\{\|#(\d+):(.*?)\|\}")]
    private static partial Regex MarkerRegex();
}
