using DotCelery.Dashboard.Controllers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Security;

/// <summary>
/// Protects the dashboard API's state-changing endpoints from cross-site requests: such a
/// request must carry the dashboard request header, and when it carries an Origin header the
/// origin must be the dashboard's own or one of <see cref="DashboardOptions.AllowedOrigins"/>.
/// </summary>
/// <remarks>
/// A browser cannot set a custom header on a cross-site request without a preflight, which a
/// same-origin policy refuses, so requiring the header stops cross-site form posts. The Origin
/// check is defense in depth: a non-browser client with the header is allowed even without one.
/// Runs in the authorization stage, before model binding.
/// </remarks>
internal sealed class DashboardRequestVerificationFilter : IAsyncAuthorizationFilter, IOrderedFilter
{
    /// <summary>
    /// The header the dashboard's own UI sends and script clients set on state-changing requests.
    /// </summary>
    public const string RequestHeaderName = "X-DotCelery-Dashboard";

    private static readonly string[] UnsafeMethods = ["POST", "PUT", "PATCH", "DELETE"];

    private readonly DashboardOptions _options;

    public DashboardRequestVerificationFilter(IOptions<DashboardOptions> options)
    {
        _options = options.Value;
    }

    /// <summary>
    /// Gets the order; the request is verified before the authorization callback runs.
    /// </summary>
    public int Order => -1000;

    public Task OnAuthorizationAsync(AuthorizationFilterContext context)
    {
        if (
            !_options.VerifyRequestOrigin
            || !DashboardAuthorizationFilter.IsDashboardAction(context)
            || !UnsafeMethods.Contains(
                context.HttpContext.Request.Method,
                StringComparer.OrdinalIgnoreCase
            )
        )
        {
            return Task.CompletedTask;
        }

        var request = context.HttpContext.Request;

        if (!request.Headers.ContainsKey(RequestHeaderName))
        {
            context.Result = new StatusCodeResult(StatusCodes.Status403Forbidden);
            return Task.CompletedTask;
        }

        var origin = request.Headers.Origin.Count > 0 ? request.Headers.Origin[0] : null;
        if (origin is null || IsAllowedOrigin(origin, request))
        {
            return Task.CompletedTask;
        }

        context.Result = new StatusCodeResult(StatusCodes.Status403Forbidden);
        return Task.CompletedTask;
    }

    private bool IsAllowedOrigin(string origin, HttpRequest request) =>
        IsSameOrigin(origin, request)
        || _options.AllowedOrigins.Any(allowed => OriginsEqual(allowed, origin));

    private static bool IsSameOrigin(string origin, HttpRequest request)
    {
        if (!Uri.TryCreate(origin, UriKind.Absolute, out var uri))
        {
            return false;
        }

        var requestPort = request.Host.Port ?? DefaultPort(request.Scheme);

        return string.Equals(uri.Scheme, request.Scheme, StringComparison.OrdinalIgnoreCase)
            && string.Equals(uri.Host, request.Host.Host, StringComparison.OrdinalIgnoreCase)
            && EffectivePort(uri) == requestPort;
    }

    private static bool OriginsEqual(string left, string right)
    {
        if (!Uri.TryCreate(left, UriKind.Absolute, out var leftUri))
        {
            return false;
        }

        if (!Uri.TryCreate(right, UriKind.Absolute, out var rightUri))
        {
            return false;
        }

        return string.Equals(leftUri.Scheme, rightUri.Scheme, StringComparison.OrdinalIgnoreCase)
            && string.Equals(leftUri.Host, rightUri.Host, StringComparison.OrdinalIgnoreCase)
            && EffectivePort(leftUri) == EffectivePort(rightUri);
    }

    private static int EffectivePort(Uri uri) =>
        uri.IsDefaultPort ? DefaultPort(uri.Scheme) : uri.Port;

    private static int DefaultPort(string scheme) =>
        string.Equals(scheme, "https", StringComparison.OrdinalIgnoreCase) ? 443 : 80;
}
