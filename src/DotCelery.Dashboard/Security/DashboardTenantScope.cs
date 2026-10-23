using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Security;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Security;

internal static class DashboardTenantScope
{
    public static string? Resolve(
        HttpContext context,
        DashboardOptions dashboard,
        MultiTenancyOptions tenancy,
        MessageSecurityOptions security
    )
    {
        if (!tenancy.Enabled || !tenancy.RequireAuthenticatedTenants)
            return null;
        var tenant = context.User.FindFirst(dashboard.TenantIdClaimType)?.Value;
        if (
            context.User.Identity?.IsAuthenticated != true
            || string.IsNullOrEmpty(tenant)
            || !security.TenantSigningKeys.ContainsKey(tenant)
            || (tenancy.ValidTenants.Count > 0 && !tenancy.ValidTenants.Contains(tenant))
        )
            throw new UnauthorizedAccessException("An authenticated tenant principal is required.");
        return tenant;
    }

    public static string Group(string name) =>
        TenantContext.Current.TenantId is { } tenant
            ? $"tenant:{tenant.Length}:{tenant}:{name}"
            : name;
}

internal sealed class DashboardTenantFilter(
    IOptions<DashboardOptions> dashboard,
    IOptions<MultiTenancyOptions> tenancy,
    IOptions<MessageSecurityOptions> security,
    IServiceProvider services
) : IAsyncResourceFilter
{
    public async Task OnResourceExecutionAsync(
        ResourceExecutingContext context,
        ResourceExecutionDelegate next
    )
    {
        if (!DashboardAuthorizationFilter.IsDashboardAction(context))
        {
            await next().ConfigureAwait(false);
            return;
        }
        string? tenant;
        try
        {
            tenant = DashboardTenantScope.Resolve(
                context.HttpContext,
                dashboard.Value,
                tenancy.Value,
                security.Value
            );
        }
        catch (UnauthorizedAccessException)
        {
            context.Result = new StatusCodeResult(StatusCodes.Status403Forbidden);
            return;
        }
        using var scope = TenantContext.SetTenant(
            tenant,
            tenant is null ? null : services.GetRequiredService<ITenantRouter>().GetQueue(tenant)
        );
        await next().ConfigureAwait(false);
    }
}

internal sealed class DashboardTenantHubFilter(
    IOptions<DashboardOptions> dashboard,
    IOptions<MultiTenancyOptions> tenancy,
    IOptions<MessageSecurityOptions> security,
    IServiceProvider services
) : IHubFilter
{
    public async ValueTask<object?> InvokeMethodAsync(
        HubInvocationContext context,
        Func<HubInvocationContext, ValueTask<object?>> next
    )
    {
        var http =
            context.Context.GetHttpContext() ?? throw new HubException("HTTP context is required.");
        if (
            !await DashboardAuthorization
                .IsAuthorizedAsync(http, dashboard.Value)
                .ConfigureAwait(false)
        )
            throw new HubException("Access denied.");
        string? tenant;
        try
        {
            tenant = DashboardTenantScope.Resolve(
                http,
                dashboard.Value,
                tenancy.Value,
                security.Value
            );
        }
        catch (UnauthorizedAccessException)
        {
            throw new HubException("Access denied.");
        }
        using var scope = TenantContext.SetTenant(
            tenant,
            tenant is null ? null : services.GetRequiredService<ITenantRouter>().GetQueue(tenant)
        );
        return await next(context).ConfigureAwait(false);
    }

    public async Task OnConnectedAsync(
        HubLifetimeContext context,
        Func<HubLifetimeContext, Task> next
    )
    {
        var http =
            context.Context.GetHttpContext() ?? throw new HubException("HTTP context is required.");
        string? tenant;
        try
        {
            tenant = DashboardTenantScope.Resolve(
                http,
                dashboard.Value,
                tenancy.Value,
                security.Value
            );
        }
        catch (UnauthorizedAccessException)
        {
            context.Context.Abort();
            return;
        }
        using var scope = TenantContext.SetTenant(
            tenant,
            tenant is null ? null : services.GetRequiredService<ITenantRouter>().GetQueue(tenant)
        );
        await next(context).ConfigureAwait(false);
    }
}
