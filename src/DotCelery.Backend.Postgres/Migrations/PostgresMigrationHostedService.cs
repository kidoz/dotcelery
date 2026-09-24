using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Backend.Postgres.Migrations;

/// <summary>
/// Applies pending migrations while the host is starting, before any hosted service
/// (such as the worker) starts.
/// </summary>
internal sealed class PostgresMigrationHostedService : IHostedLifecycleService
{
    private readonly PostgresMigrator _migrator;
    private readonly PostgresMigrationOptions _options;
    private readonly ILogger<PostgresMigrationHostedService> _logger;

    public PostgresMigrationHostedService(
        PostgresMigrator migrator,
        IOptions<PostgresMigrationOptions> options,
        ILogger<PostgresMigrationHostedService> logger
    )
    {
        _migrator = migrator;
        _options = options.Value;
        _logger = logger;
    }

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        if (!_options.RunAtStartup)
        {
            _logger.LogInformation("PostgreSQL migrations at startup are disabled");
            return;
        }

        await _migrator.MigrateAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
