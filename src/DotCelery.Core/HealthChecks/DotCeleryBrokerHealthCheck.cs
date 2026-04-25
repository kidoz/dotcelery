using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DotCelery.Core.HealthChecks;

/// <summary>
/// Reports whether the registered <see cref="IMessageBroker"/> is reachable.
/// Suitable for Kubernetes readiness probes.
/// </summary>
public sealed class DotCeleryBrokerHealthCheck(IMessageBroker broker) : IHealthCheck
{
    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            var healthy = await broker.IsHealthyAsync(cancellationToken).ConfigureAwait(false);
            return healthy
                ? HealthCheckResult.Healthy("Broker connection is healthy.")
                : HealthCheckResult.Unhealthy("Broker reported unhealthy.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Broker health probe threw an exception.", ex);
        }
    }
}
