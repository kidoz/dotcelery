namespace DotCelery.Dashboard.Security;

internal static class DashboardAuthorization
{
    public static async Task<bool> IsAuthorizedAsync(HttpContext context, DashboardOptions options)
    {
        if (options.RequireAuthorization)
        {
            return options.AuthorizationCallback is not null
                && await options.AuthorizationCallback(context).ConfigureAwait(false);
        }

        return options.AuthorizationCallback is null
            || await options.AuthorizationCallback(context).ConfigureAwait(false);
    }
}
