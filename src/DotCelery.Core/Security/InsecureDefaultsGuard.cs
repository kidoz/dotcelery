using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DotCelery.Core.Security;

/// <summary>
/// Helper for emitting startup warnings when a backend or broker is still using the
/// out-of-the-box dev connection string in a non-development environment. Library
/// defaults (cleartext localhost, well-known credentials) are appropriate for a
/// developer laptop but are unsafe in production.
/// </summary>
public static class InsecureDefaultsGuard
{
    /// <summary>
    /// Logs a warning if <paramref name="configuredValue"/> equals the built-in
    /// <paramref name="developmentDefault"/> and the host is not in the Development
    /// environment. No-op otherwise.
    /// </summary>
    /// <param name="logger">Logger to write the warning to.</param>
    /// <param name="environment">Host environment (used to skip Development).</param>
    /// <param name="componentName">Friendly component name (e.g. "RabbitMQ broker").</param>
    /// <param name="configuredValue">The currently-configured connection string.</param>
    /// <param name="developmentDefault">The built-in dev default this component ships with.</param>
    public static void WarnIfDevelopmentDefault(
        ILogger logger,
        IHostEnvironment environment,
        string componentName,
        string? configuredValue,
        string developmentDefault
    )
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentException.ThrowIfNullOrEmpty(componentName);
        ArgumentException.ThrowIfNullOrEmpty(developmentDefault);

        if (environment.IsDevelopment())
        {
            return;
        }

        if (!string.Equals(configuredValue, developmentDefault, StringComparison.Ordinal))
        {
            return;
        }

        logger.LogWarning(
            "{Component} is using the built-in development connection string in the '{Environment}' environment. "
                + "This is insecure: the default uses cleartext transport and/or well-known credentials. "
                + "Override the connection string before deploying to production.",
            componentName,
            environment.EnvironmentName
        );
    }
}
