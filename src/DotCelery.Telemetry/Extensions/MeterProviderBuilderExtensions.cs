using OpenTelemetry.Metrics;

namespace DotCelery.Telemetry.Extensions;

/// <summary>
/// Extension methods for configuring DotCelery metrics.
/// </summary>
public static class MeterProviderBuilderExtensions
{
    /// <summary>
    /// Adds DotCelery instrumentation to the meter provider.
    /// </summary>
    /// <param name="builder">The meter provider builder.</param>
    /// <returns>The builder for chaining.</returns>
    public static MeterProviderBuilder AddDotCeleryInstrumentation(
        this MeterProviderBuilder builder
    )
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddMeter(DotCeleryInstrumentation.InstrumentationName);
    }
}
