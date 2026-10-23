using System.Text;
using System.Web;
using DotCelery.Dashboard.Security;
using Microsoft.Extensions.Options;

namespace DotCelery.Dashboard.Middleware;

/// <summary>
/// Middleware that serves the DotCelery dashboard.
/// </summary>
public sealed class DashboardMiddleware
{
    private readonly RequestDelegate _next;
    private readonly DashboardOptions _options;
    private readonly Dictionary<string, (string ContentType, byte[] Content)> _embeddedResources =
        new();

    /// <summary>
    /// Initializes a new instance of the <see cref="DashboardMiddleware"/> class.
    /// </summary>
    public DashboardMiddleware(RequestDelegate next, IOptions<DashboardOptions> options)
    {
        _next = next;
        _options = options.Value;

        LoadEmbeddedResources();
    }

    /// <summary>
    /// Processes the request.
    /// </summary>
    public async Task InvokeAsync(HttpContext context)
    {
        var path = context.Request.Path.Value ?? "";
        var prefix = _options.NormalizedPathPrefix;

        // Check if this is a dashboard request; the prefix must be a whole path segment, so a
        // path like /celeryjs is not the dashboard
        if (!IsUnderPrefix(path, prefix))
        {
            await _next(context);
            return;
        }

        // The API and hub paths under the prefix are served by their endpoints, not by the UI
        // page, so they are passed on even when this middleware runs before routing
        if (IsEndpointPath(path, prefix))
        {
            await _next(context);
            return;
        }

        var isAuthorized = await DashboardAuthorization
            .IsAuthorizedAsync(context, _options)
            .ConfigureAwait(false);
        if (!isAuthorized)
        {
            context.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        // Get the path after the prefix
        var subPath = path[prefix.Length..];
        if (string.IsNullOrEmpty(subPath) || subPath == "/")
        {
            subPath = "/index.html";
        }

        // Try to serve embedded resource
        if (_embeddedResources.TryGetValue(subPath.ToLowerInvariant(), out var resource))
        {
            context.Response.ContentType = resource.ContentType;
            context.Response.ContentLength = resource.Content.Length;
            await context.Response.Body.WriteAsync(resource.Content);
            return;
        }

        // For SPA routing, serve index.html for non-file paths
        if (
            !subPath.Contains('.')
            && _embeddedResources.TryGetValue("/index.html", out var indexResource)
        )
        {
            context.Response.ContentType = indexResource.ContentType;
            context.Response.ContentLength = indexResource.Content.Length;
            await context.Response.Body.WriteAsync(indexResource.Content);
            return;
        }

        // Not found
        context.Response.StatusCode = StatusCodes.Status404NotFound;
    }

    private static bool IsUnderPrefix(string path, string prefix) =>
        path.Equals(prefix, StringComparison.OrdinalIgnoreCase)
        || path.StartsWith(prefix + "/", StringComparison.OrdinalIgnoreCase);

    private static bool IsEndpointPath(string path, string prefix)
    {
        var subPath = path[prefix.Length..];

        return IsSegment(subPath, "/api") || IsSegment(subPath, "/hub");

        static bool IsSegment(string subPath, string segment) =>
            subPath.Equals(segment, StringComparison.OrdinalIgnoreCase)
            || subPath.StartsWith(segment + "/", StringComparison.OrdinalIgnoreCase);
    }

    private void LoadEmbeddedResources()
    {
        var assembly = typeof(DashboardMiddleware).Assembly;
        var resourcePrefix = "DotCelery.Dashboard.wwwroot.";

        foreach (var resourceName in assembly.GetManifestResourceNames())
        {
            if (!resourceName.StartsWith(resourcePrefix, StringComparison.Ordinal))
            {
                continue;
            }

            // Extract the path after the prefix
            // Resource names look like: DotCelery.Dashboard.wwwroot.js.app.js
            // We need to convert to: /js/app.js
            var pathPart = resourceName[resourcePrefix.Length..];
            var relativePath = ConvertResourceNameToPath(pathPart);

            using var stream = assembly.GetManifestResourceStream(resourceName);
            if (stream is null)
            {
                continue;
            }

            using var ms = new MemoryStream();
            stream.CopyTo(ms);
            var content = ms.ToArray();

            var contentType = GetContentType(relativePath);
            _embeddedResources[relativePath] = (contentType, content);
        }

        if (!_embeddedResources.TryGetValue("/index.html", out var index))
        {
            throw new InvalidOperationException("The embedded dashboard page is missing.");
        }
        var html = Encoding
            .UTF8.GetString(index.Content)
            .Replace("{{TITLE}}", HttpUtility.HtmlEncode(_options.Title), StringComparison.Ordinal)
            .Replace(
                "{{PREFIX}}",
                HttpUtility.HtmlEncode(_options.NormalizedPathPrefix),
                StringComparison.Ordinal
            );
        _embeddedResources["/index.html"] = (index.ContentType, Encoding.UTF8.GetBytes(html));
    }

    /// <summary>
    /// Converts a resource name path part to a URL path.
    /// Resource names use dots as separators, but the last segment before the extension
    /// contains the actual filename with its extension.
    /// Example: "js.app.js" -> "/js/app.js", "index.html" -> "/index.html"
    /// </summary>
    private static string ConvertResourceNameToPath(string resourcePath)
    {
        // Split by dots
        var parts = resourcePath.Split('.');

        if (parts.Length < 2)
        {
            // No extension, just return as-is
            return "/" + resourcePath.ToLowerInvariant();
        }

        // The last part is the extension (e.g., "js", "html", "css")
        var extension = parts[^1];

        // The second-to-last part is the filename without extension
        var fileName = parts[^2];

        // Everything before that forms the directory path
        var directoryParts = parts[..^2];

        var path =
            directoryParts.Length > 0
                ? "/" + string.Join("/", directoryParts) + "/" + fileName + "." + extension
                : "/" + fileName + "." + extension;

        return path.ToLowerInvariant();
    }

    private static string GetContentType(string path) =>
        Path.GetExtension(path).ToLowerInvariant() switch
        {
            ".html" => "text/html; charset=utf-8",
            ".css" => "text/css; charset=utf-8",
            ".js" => "application/javascript; charset=utf-8",
            ".json" => "application/json; charset=utf-8",
            ".png" => "image/png",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".gif" => "image/gif",
            ".svg" => "image/svg+xml",
            ".ico" => "image/x-icon",
            ".woff" => "font/woff",
            ".woff2" => "font/woff2",
            _ => "application/octet-stream",
        };
}
