using System.Diagnostics;

namespace DotCelery.Core.Instrumentation;

/// <summary>
/// Diagnostic primitives shared across DotCelery components.
/// </summary>
/// <remarks>
/// The <see cref="ActivitySource"/> is named <c>DotCelery</c>. Register it with
/// OpenTelemetry via <c>AddSource("DotCelery")</c> — the <c>DotCelery.Telemetry</c>
/// package exposes helpers that do this for you.
/// </remarks>
public static class DotCeleryDiagnostics
{
    /// <summary>
    /// Logical name of the instrumentation source.
    /// </summary>
    public const string SourceName = "DotCelery";

    /// <summary>
    /// Logical version of the instrumentation source.
    /// </summary>
    public const string SourceVersion = "1.0.0";

    /// <summary>
    /// The <see cref="ActivitySource"/> used for DotCelery produce/consume spans.
    /// </summary>
    public static ActivitySource ActivitySource { get; } = new(SourceName, SourceVersion);
}
