using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DotCelery.Core.HealthChecks;

/// <summary>
/// Extension methods for registering DotCelery health checks.
/// </summary>
public static class HealthChecksBuilderExtensions
{
    /// <summary>
    /// Default health-check name for the broker probe.
    /// </summary>
    public const string BrokerCheckName = "dotcelery-broker";

    /// <summary>
    /// Default health-check name for the result backend probe.
    /// </summary>
    public const string BackendCheckName = "dotcelery-backend";

    /// <summary>
    /// Registers broker and result-backend health checks. Call this on an
    /// <see cref="IHealthChecksBuilder"/> obtained from <c>IServiceCollection.AddHealthChecks()</c>.
    /// </summary>
    /// <param name="builder">The health-checks builder.</param>
    /// <param name="failureStatus">Status to report on failure; defaults to <see cref="HealthStatus.Unhealthy"/>.</param>
    /// <param name="tags">Optional tags applied to each registered check.</param>
    /// <returns>The builder for chaining.</returns>
    public static IHealthChecksBuilder AddDotCelery(
        this IHealthChecksBuilder builder,
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);

        var tagList = tags?.ToArray();

        builder.AddCheck<DotCeleryBrokerHealthCheck>(
            BrokerCheckName,
            failureStatus,
            tagList ?? Array.Empty<string>()
        );

        builder.AddCheck<DotCeleryBackendHealthCheck>(
            BackendCheckName,
            failureStatus,
            tagList ?? Array.Empty<string>()
        );

        return builder;
    }
}
