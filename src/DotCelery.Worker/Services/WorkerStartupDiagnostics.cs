using DotCelery.Core.Filters;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Security;
using DotCelery.Worker.Filters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Services;

/// <summary>
/// Reports security settings that are not enabled when the worker starts, so an application
/// does not discover them only after a message it should have refused was accepted.
/// </summary>
internal sealed class WorkerStartupDiagnostics(
    IServiceProvider services,
    IOptions<MessageSecurityOptions> securityOptions,
    IOptions<MultiTenancyOptions> tenancyOptions,
    IOptions<TaskFilterOptions> filterOptions,
    ILogger<WorkerStartupDiagnostics> logger
) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var security = securityOptions.Value;

        if (!security.EnableMessageSigning)
        {
            logger.LogWarning(
                "Message signing is disabled, so the worker cannot distinguish a message from "
                    + "another producer from a forged one. Enable it with "
                    + "UseMessageSecurity(options => options.EnableMessageSigning = true)."
            );
        }

        if (security.MaxPayloadSizeBytes <= 0)
        {
            logger.LogWarning(
                "Payload size validation is disabled. Set "
                    + "MessageSecurityOptions.MaxPayloadSizeBytes to bound the payload a message "
                    + "may carry."
            );
        }

        if (!security.EnforceTaskAllowlist)
        {
            logger.LogInformation(
                "The task allowlist is disabled, so every task name the worker registers can be "
                    + "invoked. Set MessageSecurityOptions.EnforceTaskAllowlist and "
                    + "AllowedTaskNames to allow only the listed names."
            );
        }

        var tenancy = tenancyOptions.Value;

        if (tenancy.ValidateTenants && tenancy.ValidTenants.Count == 0)
        {
            throw new InvalidOperationException(
                "Multi-tenancy validation is enabled but MultiTenancyOptions.ValidTenants is "
                    + "empty, so no tenant would be accepted. Add the valid tenants or disable "
                    + "validation."
            );
        }

        if (tenancy.Enabled && tenancy.ValidTenants.Count == 0)
        {
            logger.LogWarning(
                "Multi-tenancy is enabled without valid tenants, so every tenant is accepted. "
                    + "Set MultiTenancyOptions.ValidTenants to reject unknown tenants."
            );
        }

        var tenantFilterRegistered = filterOptions.Value.GlobalFilterTypes.Contains(
            typeof(TenantContextFilter)
        );

        if (!tenantFilterRegistered && services.GetService<ITenantRouter>() is not null)
        {
            logger.LogWarning(
                "Tenant routing is configured but UseTenantContext() is not, so a message's "
                    + "tenant is not validated before its task runs."
            );
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
