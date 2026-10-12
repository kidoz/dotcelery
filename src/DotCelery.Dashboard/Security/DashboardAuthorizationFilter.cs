using DotCelery.Dashboard.Controllers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Security;

/// <summary>
/// Authorizes dashboard API requests in the authorization stage, before model binding, so a
/// refused request is never bound or executed.
/// </summary>
internal sealed class DashboardAuthorizationFilter : IAsyncAuthorizationFilter
{
    private readonly DashboardOptions _options;

    public DashboardAuthorizationFilter(IOptions<DashboardOptions> options)
    {
        _options = options.Value;
    }

    public async Task OnAuthorizationAsync(AuthorizationFilterContext context)
    {
        if (!IsDashboardAction(context))
        {
            return;
        }

        var isAuthorized = await DashboardAuthorization
            .IsAuthorizedAsync(context.HttpContext, _options)
            .ConfigureAwait(false);

        if (!isAuthorized)
        {
            context.Result = new StatusCodeResult(StatusCodes.Status403Forbidden);
        }
    }

    // At the authorization stage the controller is not created yet, so the action descriptor
    // identifies a dashboard action
    internal static bool IsDashboardAction(FilterContext context) =>
        context.ActionDescriptor is ControllerActionDescriptor descriptor
        && descriptor.ControllerTypeInfo.AsType() == typeof(DashboardApiController);
}
