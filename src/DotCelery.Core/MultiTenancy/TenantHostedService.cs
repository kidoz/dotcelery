using System.Diagnostics.CodeAnalysis;
using DotCelery.Core.Security;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace DotCelery.Core.MultiTenancy;

/// <summary>Runs an independent background service instance inside each configured tenant scope.</summary>
/// <typeparam name="T">The background service with tenant-local cursors and caches.</typeparam>
public sealed class TenantHostedService<
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T
>(
    IServiceProvider services,
    IOptions<MultiTenancyOptions> tenancy,
    IOptions<MessageSecurityOptions> security
) : BackgroundService
    where T : BackgroundService
{
    private readonly List<(string? Tenant, T Service)> _children = [];

    /// <inheritdoc />
    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        IEnumerable<string?> tenants = tenancy.Value
            is { Enabled: true, RequireAuthenticatedTenants: true }
            ? security.Value.TenantSigningKeys.Keys
            : new string?[] { null };
        foreach (var tenant in tenants)
        {
            using var scope = TenantContext.SetTenant(
                tenant,
                tenant is null
                    ? null
                    : services.GetRequiredService<ITenantRouter>().GetQueue(tenant)
            );
            var child = ActivatorUtilities.CreateInstance<T>(services);
            _children.Add((tenant, child));
            await child.StartAsync(cancellationToken).ConfigureAwait(false);
        }
        await base.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(_children.Select(child => child.Service.ExecuteTask ?? Task.CompletedTask));

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var (tenant, child) in _children)
        {
            using var scope = TenantContext.SetTenant(
                tenant,
                tenant is null
                    ? null
                    : services.GetRequiredService<ITenantRouter>().GetQueue(tenant)
            );
            await child.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        foreach (var (_, child) in _children)
            (child as IDisposable)?.Dispose();
        base.Dispose();
    }
}
