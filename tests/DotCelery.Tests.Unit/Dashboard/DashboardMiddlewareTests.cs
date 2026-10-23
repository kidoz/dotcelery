using System.Text;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Middleware;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Sends requests through the dashboard middleware, so the UI page, the API and hub pass-through,
/// and the prefix boundary are proven without a running server.
/// </summary>
public sealed class DashboardMiddlewareTests
{
    [Theory]
    [InlineData("/celery/api/overview")]
    [InlineData("/celery/api/v1/overview")]
    [InlineData("/celery/api/v2/overview")]
    [InlineData("/celery/api")]
    [InlineData("/celery/hub/negotiate")]
    [InlineData("/celery/hub")]
    [InlineData("/outside")]
    [InlineData("/celeryjs")]
    public async Task EndpointAndForeignPaths_PassThrough(string path)
    {
        var nextCalled = false;
        var middleware = CreateMiddleware(() => nextCalled = true);
        var context = CreateContext(path);

        await middleware.InvokeAsync(context);

        Assert.True(nextCalled);
    }

    [Theory]
    [InlineData("/celery")]
    [InlineData("/celery/")]
    [InlineData("/celery/tasks/123")]
    public async Task UiPaths_ServeTheDashboardPage(string path)
    {
        var nextCalled = false;
        var middleware = CreateMiddleware(() => nextCalled = true);
        var context = CreateContext(path);

        await middleware.InvokeAsync(context);

        Assert.False(nextCalled);
        Assert.StartsWith("text/html", context.Response.ContentType);
        Assert.NotEmpty(Encoding.UTF8.GetString(GetBody(context)));
    }

    [Theory]
    [InlineData(".css", "text/css")]
    [InlineData(".js", "application/javascript")]
    [InlineData(".png", "image/png")]
    public async Task EmbeddedAssets_AreServedUnderTheConfiguredPrefix(
        string extension,
        string contentType
    )
    {
        var middleware = new DashboardMiddleware(
            _ => Task.CompletedTask,
            Options.Create(
                new DashboardOptions { RequireAuthorization = false, PathPrefix = "/jobs/monitor" }
            )
        );
        const string resourcePrefix = "DotCelery.Dashboard.wwwroot/";
        var resources = typeof(DashboardMiddleware).Assembly.GetManifestResourceNames();
        var assets = resources
            .Where(name =>
                name.StartsWith(resourcePrefix, StringComparison.Ordinal)
                && name.EndsWith(extension, StringComparison.Ordinal)
            )
            .ToList();
        Assert.NotEmpty(assets);
        foreach (var resource in assets)
        {
            var asset = resource[resourcePrefix.Length..].Replace('\\', '/');
            var context = CreateContext($"/jobs/monitor/{asset}");
            await middleware.InvokeAsync(context);
            Assert.StartsWith(contentType, context.Response.ContentType);
            Assert.NotEmpty(GetBody(context));
        }
    }

    [Fact]
    public async Task TitleAndAssetPrefix_AreEscapedInTheEmbeddedPage()
    {
        var middleware = new DashboardMiddleware(
            _ => Task.CompletedTask,
            Options.Create(
                new DashboardOptions
                {
                    RequireAuthorization = false,
                    PathPrefix = "/jobs/monitor",
                    Title = "<script>alert(1)</script>",
                }
            )
        );
        var context = CreateContext("/jobs/monitor/");
        await middleware.InvokeAsync(context);
        var html = Encoding.UTF8.GetString(GetBody(context));
        Assert.Contains("&lt;script&gt;alert(1)&lt;/script&gt;", html);
        Assert.DoesNotContain("<script>alert(1)</script>", html);
        Assert.Contains("<base href=\"/jobs/monitor/\"", html);
        Assert.Contains("./assets/", html);
        Assert.Contains("type=\"module\"", html);
        Assert.Contains("data-title=\"&lt;script&gt;", html);
        Assert.DoesNotContain("{{PREFIX}}", html);
        Assert.DoesNotContain("{{TITLE}}", html);
        Assert.Contains("<noscript", html);
    }

    private static DashboardMiddleware CreateMiddleware(Action onNext) =>
        new(
            _ =>
            {
                onNext();
                return Task.CompletedTask;
            },
            Options.Create(new DashboardOptions { RequireAuthorization = false })
        );

    private static DefaultHttpContext CreateContext(string path)
    {
        var context = new DefaultHttpContext();
        context.Request.Path = path;
        context.Response.Body = new MemoryStream();
        return context;
    }

    private static byte[] GetBody(HttpContext context)
    {
        context.Response.Body.Position = 0;
        using var reader = new MemoryStream();
        context.Response.Body.CopyTo(reader);
        return reader.ToArray();
    }
}
