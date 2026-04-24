using DotCelery.Dashboard.Controllers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Security;

internal sealed class DashboardAuthorizationFilter : IAsyncActionFilter
{
    private readonly DashboardOptions _options;

    public DashboardAuthorizationFilter(IOptions<DashboardOptions> options)
    {
        _options = options.Value;
    }

    public async Task OnActionExecutionAsync(
        ActionExecutingContext context,
        ActionExecutionDelegate next
    )
    {
        if (context.Controller is not DashboardApiController)
        {
            await next().ConfigureAwait(false);
            return;
        }

        var isAuthorized = await DashboardAuthorization
            .IsAuthorizedAsync(context.HttpContext, _options)
            .ConfigureAwait(false);

        if (!isAuthorized)
        {
            context.Result = new StatusCodeResult(StatusCodes.Status403Forbidden);
            return;
        }

        await next().ConfigureAwait(false);
    }
}
