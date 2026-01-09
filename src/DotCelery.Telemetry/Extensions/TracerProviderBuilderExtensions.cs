using OpenTelemetry.Trace;

namespace DotCelery.Telemetry.Extensions;

/// <summary>
/// Extension methods for configuring DotCelery tracing.
/// </summary>
public static class TracerProviderBuilderExtensions
{
    /// <summary>
    /// Adds DotCelery instrumentation to the tracer provider.
    /// </summary>
    /// <param name="builder">The tracer provider builder.</param>
    /// <returns>The builder for chaining.</returns>
    public static TracerProviderBuilder AddDotCeleryInstrumentation(
        this TracerProviderBuilder builder
    )
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddSource(DotCeleryInstrumentation.InstrumentationName);
    }
}
