using DotCelery.Core.Abstractions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace DotCelery.Core.HealthChecks;

/// <summary>
/// Reports whether the registered <see cref="IResultBackend"/> responds to queries.
/// Issues a read for a sentinel task id — a null result means the backend is reachable
/// and the query round-trip completed successfully.
/// </summary>
public sealed class DotCeleryBackendHealthCheck(IResultBackend backend) : IHealthCheck
{
    private const string SentinelTaskId = "__dotcelery_healthcheck__";

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            _ = await backend.GetResultAsync(SentinelTaskId, cancellationToken)
                .ConfigureAwait(false);
            return HealthCheckResult.Healthy("Result backend is reachable.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(
                "Result backend health probe threw an exception.",
                ex
            );
        }
    }
}
