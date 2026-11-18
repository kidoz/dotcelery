using Microsoft.AspNetCore.Mvc.Filters;

namespace DotCelery.Dashboard.Security;

[AttributeUsage(AttributeTargets.Class)]
internal sealed class DashboardResponseHeadersAttribute : Attribute, IAlwaysRunResultFilter
{
    public void OnResultExecuting(ResultExecutingContext context)
    {
        context.HttpContext.Response.Headers.CacheControl = "no-store";
        context.HttpContext.Response.Headers.XContentTypeOptions = "nosniff";
    }

    public void OnResultExecuted(ResultExecutedContext context) { }
}
