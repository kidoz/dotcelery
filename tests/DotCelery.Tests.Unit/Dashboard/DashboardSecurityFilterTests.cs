using System.Reflection;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Controllers;
using DotCelery.Dashboard.Security;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Controllers;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Exercises the dashboard's security filters: authorization runs in the authorization stage
/// (before model binding), and state-changing requests must carry the dashboard request header
/// and come from an allowed origin.
/// </summary>
public sealed class DashboardSecurityFilterTests
{
    [Fact]
    public async Task Authorization_WithoutACallback_DeniesDashboardActionsAndPassesOthers()
    {
        var filter = new DashboardAuthorizationFilter(
            Options.Create(new DashboardOptions { RequireAuthorization = true })
        );

        var dashboard = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        await filter.OnAuthorizationAsync(dashboard);
        Assert.Equal(StatusCodes.Status403Forbidden, Result(dashboard)?.StatusCode);

        // Another application part's controller is not authorized by the dashboard
        var foreign = CreateAuthorizationContext(
            "POST",
            "/orders",
            typeof(DashboardSecurityFilterTests)
        );
        await filter.OnAuthorizationAsync(foreign);
        Assert.Null(Result(foreign));
    }

    [Fact]
    public void Authorization_FilterRunsInTheAuthorizationStage()
    {
        // MVC runs authorization filters before model binding, which is what keeps a refused
        // request from being bound; the endpoint test proves the ordering end to end
        var filter = new DashboardAuthorizationFilter(
            Options.Create(new DashboardOptions { RequireAuthorization = true })
        );

        Assert.IsAssignableFrom<IAsyncAuthorizationFilter>(filter);
        Assert.IsNotAssignableFrom<IAsyncActionFilter>(filter);
    }

    [Fact]
    public async Task Authorization_WithACallback_FollowsIt()
    {
        var allowed = new DashboardAuthorizationFilter(
            Options.Create(
                new DashboardOptions
                {
                    RequireAuthorization = true,
                    AuthorizationCallback = _ => Task.FromResult(true),
                }
            )
        );
        var denied = new DashboardAuthorizationFilter(
            Options.Create(
                new DashboardOptions
                {
                    RequireAuthorization = true,
                    AuthorizationCallback = _ => Task.FromResult(false),
                }
            )
        );

        var allowedContext = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        await allowed.OnAuthorizationAsync(allowedContext);
        Assert.Null(Result(allowedContext));

        var deniedContext = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        await denied.OnAuthorizationAsync(deniedContext);
        Assert.Equal(StatusCodes.Status403Forbidden, Result(deniedContext)?.StatusCode);
    }

    [Fact]
    public async Task StateChanges_WithoutTheRequestHeader_AreRefused()
    {
        var filter = CreateVerificationFilter();

        var context = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        await filter.OnAuthorizationAsync(context);

        Assert.Equal(StatusCodes.Status403Forbidden, Result(context)?.StatusCode);
    }

    [Fact]
    public async Task StateChanges_FromACrossOrigin_AreRefused()
    {
        var filter = CreateVerificationFilter();

        var context = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        context.HttpContext.Request.Headers[DashboardRequestVerificationFilter.RequestHeaderName] =
            "1";
        context.HttpContext.Request.Headers.Origin = "https://evil.example";

        await filter.OnAuthorizationAsync(context);

        Assert.Equal(StatusCodes.Status403Forbidden, Result(context)?.StatusCode);
    }

    [Theory]
    [InlineData("https://dashboard.example")] // the dashboard's own origin
    [InlineData("http://localhost:5000")] // the request origin, with the default port
    public async Task StateChanges_FromAnAllowedOrigin_AreAccepted(string origin)
    {
        var filter = CreateVerificationFilter(allowedOrigins: ["https://dashboard.example"]);

        var context = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        context.HttpContext.Request.Scheme = "http";
        context.HttpContext.Request.Host = new HostString("localhost", 5000);
        context.HttpContext.Request.Headers[DashboardRequestVerificationFilter.RequestHeaderName] =
            "1";
        context.HttpContext.Request.Headers.Origin = origin;

        await filter.OnAuthorizationAsync(context);

        Assert.Null(Result(context));
    }

    [Fact]
    public async Task StateChanges_FromAScriptClientWithoutAnOrigin_AreAccepted()
    {
        var filter = CreateVerificationFilter();

        var context = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        context.HttpContext.Request.Headers[DashboardRequestVerificationFilter.RequestHeaderName] =
            "1";

        await filter.OnAuthorizationAsync(context);

        Assert.Null(Result(context));
    }

    [Fact]
    public async Task Reads_DoNotNeedTheRequestHeader()
    {
        var filter = CreateVerificationFilter();

        var context = CreateAuthorizationContext("GET", "/celery/api/overview");
        await filter.OnAuthorizationAsync(context);

        Assert.Null(Result(context));
    }

    [Fact]
    public async Task Verification_CanBeTurnedOff()
    {
        var filter = new DashboardRequestVerificationFilter(
            Options.Create(new DashboardOptions { VerifyRequestOrigin = false })
        );

        var context = CreateAuthorizationContext("POST", "/celery/api/tasks/t1/revoke");
        context.HttpContext.Request.Headers.Origin = "https://evil.example";

        await filter.OnAuthorizationAsync(context);

        Assert.Null(Result(context));
    }

    private static DashboardRequestVerificationFilter CreateVerificationFilter(
        IReadOnlyCollection<string>? allowedOrigins = null
    ) => new(Options.Create(new DashboardOptions { AllowedOrigins = allowedOrigins ?? [] }));

    private static StatusCodeResult? Result(AuthorizationFilterContext context) =>
        context.Result as StatusCodeResult;

    private static AuthorizationFilterContext CreateAuthorizationContext(
        string method,
        string path,
        Type? controllerType = null
    )
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Request.Method = method;
        httpContext.Request.Path = path;
        httpContext.Request.Scheme = "https";
        httpContext.Request.Host = new HostString("dashboard.example");

        var actionContext = new ActionContext(
            httpContext,
            new RouteData(),
            new ControllerActionDescriptor
            {
                ControllerTypeInfo = (
                    controllerType ?? typeof(DashboardApiController)
                ).GetTypeInfo(),
            }
        );

        return new AuthorizationFilterContext(actionContext, []);
    }
}
