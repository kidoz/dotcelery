using DotCelery.Core.Security;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Broker.Redis.Services;

/// <summary>
/// Logs a warning at startup if the Redis Streams broker is still using the
/// built-in development connection string in a non-development host environment.
/// </summary>
internal sealed class RedisBrokerInsecureDefaultsCheck(
    IOptions<RedisBrokerOptions> options,
    IHostEnvironment environment,
    ILogger<RedisBrokerInsecureDefaultsCheck> logger
) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            environment,
            componentName: "Redis Streams broker",
            configuredValue: options.Value.ConnectionString,
            developmentDefault: RedisBrokerOptions.DevelopmentDefaultConnectionString
        );
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
