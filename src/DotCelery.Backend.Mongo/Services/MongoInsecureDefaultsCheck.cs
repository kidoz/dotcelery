using DotCelery.Backend.Mongo.Storage;
using DotCelery.Core.Security;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.Mongo.Services;

internal sealed class MongoInsecureDefaultsCheck(
    IOptions<MongoStorageOptions> options,
    ILogger<MongoInsecureDefaultsCheck>? logger = null,
    IHostEnvironment? environment = null
) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (environment is null || logger is null)
            return Task.CompletedTask;
        InsecureDefaultsGuard.WarnIfDevelopmentDefault(
            logger,
            environment,
            "MongoDB storage",
            options.Value.ConnectionString,
            MongoStorageOptions.DevelopmentDefaultConnectionString
        );
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
