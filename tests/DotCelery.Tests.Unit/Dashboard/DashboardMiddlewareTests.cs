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
