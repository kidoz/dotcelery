using DotCelery.Core.Security;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Broker.RabbitMQ.Services;

/// <summary>
/// Logs a warning at startup if the RabbitMQ broker is still using the built-in
/// development connection string in a non-development host environment.
/// </summary>
internal sealed class RabbitMQInsecureDefaultsCheck(
    IOptions<RabbitMQBrokerOptions> options,
    IHostEnvironment environment,
    ILogger<RabbitMQInsecureDefaultsCheck> logger
) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            environment,
            componentName: "RabbitMQ broker",
            configuredValue: options.Value.ConnectionString,
            developmentDefault: RabbitMQBrokerOptions.DevelopmentDefaultConnectionString
        );
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
