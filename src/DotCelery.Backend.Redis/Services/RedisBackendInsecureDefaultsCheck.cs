using DotCelery.Core.Security;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.Redis.Services;

/// <summary>
/// Logs a warning at startup if the Redis backend is still using the built-in
/// development connection string in a non-development host environment.
/// </summary>
internal sealed class RedisBackendInsecureDefaultsCheck(
    IOptions<RedisBackendOptions> options,
    IHostEnvironment environment,
    ILogger<RedisBackendInsecureDefaultsCheck> logger
) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            environment,
            componentName: "Redis result backend",
            configuredValue: options.Value.ConnectionString,
            developmentDefault: RedisBackendOptions.DevelopmentDefaultConnectionString
        );
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
