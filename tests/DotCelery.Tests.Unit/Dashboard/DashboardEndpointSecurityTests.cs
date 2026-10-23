using System.Net;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using Asp.Versioning.ApiExplorer;
using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Core.Extensions;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Security;
using DotCelery.Core.Storage.Stores;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Extensions;
using DotCelery.Dashboard.Security;
using DotCelery.Dashboard.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// Runs the dashboard API on a real server, so the request protection and the order of
/// authorization and model binding are proven over the wire, not only at the filter level.
/// </summary>
public sealed class DashboardEndpointSecurityTests
{
    [Fact]
    public async Task VersionedEndpoints_ReportTheirContractAndRejectUnversionedOrUnknownVersions()
    {
        await using var host = await StartAsync(options => options.RequireAuthorization = false);
        var response = await host.Client.GetAsync("/celery/api/v1/capabilities");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("1.0", response.Headers.GetValues("api-supported-versions"));
        Assert.Equal(
            HttpStatusCode.NotFound,
            (await host.Client.GetAsync("/celery/api/capabilities")).StatusCode
        );
        Assert.Equal(
            HttpStatusCode.NotFound,
            (await host.Client.GetAsync("/celery/api/v2/capabilities")).StatusCode
        );
        Assert.Equal(
            HttpStatusCode.NotFound,
            (await host.Client.GetAsync("/celery/api/vbad/capabilities")).StatusCode
        );
    }

    [Fact]
    public async Task InvalidVersionedQueries_ReturnProblemDetails()
    {
        await using var host = await StartAsync(options => options.RequireAuthorization = false);
        var response = await host.Client.GetAsync("/celery/api/v1/tasks/search?state=NotAState");
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
        using var body = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        Assert.Equal(400, body.RootElement.GetProperty("status").GetInt32());
        Assert.True(body.RootElement.TryGetProperty("errors", out _));
    }

    [Fact]
    public async Task ApiExplorer_SubstitutesTheVersionInDocumentedRoutes()
    {
        await using var host = await StartAsync(options => options.RequireAuthorization = false);
        var versions = host.App.Services.GetRequiredService<IApiVersionDescriptionProvider>();
        Assert.Contains(versions.ApiVersionDescriptions, version => version.GroupName == "v1");
        var explorer =
            host.App.Services.GetRequiredService<IApiDescriptionGroupCollectionProvider>();
        var endpoints = explorer
            .ApiDescriptionGroups.Items.SelectMany(group => group.Items)
            .ToList();
        Assert.Contains(
            endpoints,
            endpoint =>
                endpoint.RelativePath == "celery/api/v1/capabilities" && endpoint.GroupName == "v1"
        );
        Assert.DoesNotContain(
            endpoints,
            endpoint =>
                endpoint.RelativePath?.Contains("{version", StringComparison.Ordinal) == true
        );
    }

    [Fact]
    public async Task StateChangingRequests_MustComeFromADashboardClient()
    {
        await using var host = await StartAsync(o =>
        {
            o.RequireAuthorization = false;
            o.AllowTaskOperations = true;
        });

        // Without the dashboard header the request is refused, whichever page sent it
        var withoutHeader = await host.Client.PostAsync(
            "/celery/api/v1/tasks/t1/revoke",
            JsonContent()
        );
        Assert.Equal(HttpStatusCode.Forbidden, withoutHeader.StatusCode);

        // A cross-site origin is refused even with the header
        var crossOrigin = new HttpRequestMessage(HttpMethod.Post, "/celery/api/v1/tasks/t1/revoke")
        {
            Content = JsonContent(),
        };
        crossOrigin.Headers.Add(DashboardRequestVerificationFilter.RequestHeaderName, "1");
        crossOrigin.Headers.Add("Origin", "https://evil.example");
        var crossOriginResponse = await host.Client.SendAsync(crossOrigin);
        Assert.Equal(HttpStatusCode.Forbidden, crossOriginResponse.StatusCode);

        // The dashboard's own request reaches the endpoint
        var allowed = new HttpRequestMessage(HttpMethod.Post, "/celery/api/v1/tasks/t1/revoke")
        {
            Content = JsonContent(),
        };
        allowed.Headers.Add(DashboardRequestVerificationFilter.RequestHeaderName, "1");
        var allowedResponse = await host.Client.SendAsync(allowed);
        Assert.Equal(HttpStatusCode.OK, allowedResponse.StatusCode);
    }

    [Fact]
    public async Task DeniedRequests_AreRefusedBeforeTheBodyIsBound()
    {
        await using var host = await StartAsync(o =>
        {
            o.RequireAuthorization = true;
            o.AuthorizationCallback = _ => Task.FromResult(false);
        });

        var request = new HttpRequestMessage(HttpMethod.Post, "/celery/api/v1/tasks/t1/revoke")
        {
            // Malformed JSON is a 400 when the body is bound; authorization must win first
            Content = new StringContent("{ not json", Encoding.UTF8, "application/json"),
        };
        request.Headers.Add(DashboardRequestVerificationFilter.RequestHeaderName, "1");

        var response = await host.Client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Theory]
    [InlineData("tasks/t1/revoke")]
    [InlineData("tasks/bulk/revoke")]
    [InlineData("tasks/bulk/retry")]
    public async Task ReadOnlyActions_ReturnForbiddenWithoutAnAuthenticationScheme(string path)
    {
        await using var host = await StartAsync(options =>
        {
            options.RequireAuthorization = false;
            options.ReadOnly = true;
        });
        var request = new HttpRequestMessage(HttpMethod.Post, $"/celery/api/v1/{path}")
        {
            Content = new StringContent(
                """{"taskIds":["t1"],"terminate":true}""",
                Encoding.UTF8,
                "application/json"
            ),
        };
        request.Headers.Add(DashboardRequestVerificationFilter.RequestHeaderName, "1");
        var response = await host.Client.SendAsync(request);
        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    [Fact]
    public async Task TenantClaims_ScopeDashboardQueriesAndCannotBeOverriddenByARequestHeader()
    {
        await using var host = await StartAsync(
            options => options.RequireAuthorization = false,
            isolateTenants: true
        );
        var records = host.App.Services.GetRequiredService<TaskRecordStore>();
        using (TenantContext.SetTenant("a"))
            await records.RecordAsync(
                new()
                {
                    TaskId = "same",
                    TaskName = "private-a",
                    State = "Success",
                }
            );
        using (TenantContext.SetTenant("b"))
            await records.RecordAsync(
                new()
                {
                    TaskId = "same",
                    TaskName = "private-b",
                    State = "Success",
                }
            );
        Assert.Equal(
            HttpStatusCode.Forbidden,
            (await host.Client.GetAsync("/celery/api/v1/tasks/same")).StatusCode
        );
        foreach (var tenant in new[] { "a", "b" })
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, "/celery/api/v1/tasks/same");
            request.Headers.Add("test-authenticated-tenant", tenant);
            request.Headers.Add("X-Tenant-Id", tenant == "a" ? "b" : "a");
            var response = await host.Client.SendAsync(request);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            var body = await response.Content.ReadAsStringAsync();
            Assert.Contains($"private-{tenant}", body, StringComparison.Ordinal);
            Assert.DoesNotContain(
                tenant == "a" ? "private-b" : "private-a",
                body,
                StringComparison.Ordinal
            );
        }
    }

    private static StringContent JsonContent() =>
        new("""{"terminate": true}""", Encoding.UTF8, "application/json");

    private static async Task<TestHost> StartAsync(
        Action<DashboardOptions> configure,
        bool isolateTenants = false
    )
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");

        builder.Services.AddDotCelery(celery =>
        {
            celery.UseInMemoryBroker().UseInMemoryBackend().AddInMemoryRevocationStore();
            if (isolateTenants)
                celery.AddTenantRouting(options =>
                {
                    options.Enabled = true;
                    options.ValidTenants = ["a", "b"];
                });
        });
        if (isolateTenants)
            builder.Services.Configure<MessageSecurityOptions>(options =>
            {
                options.EnableMessageSigning = true;
                options.TenantSigningKeys = new()
                {
                    ["a"] = Enumerable.Repeat((byte)1, 32).ToArray(),
                    ["b"] = Enumerable.Repeat((byte)2, 32).ToArray(),
                };
            });
        builder.Services.AddDotCeleryDashboard(o =>
        {
            o.PathPrefix = "/celery";
            configure(o);
        });

        var app = builder.Build();
        if (isolateTenants)
        {
            var router = app.Services.GetRequiredService<ITenantRouter>();
            router.AddTenantQueue("a", "queue-a");
            router.AddTenantQueue("b", "queue-b");
        }
        if (isolateTenants)
            app.Use(
                async (context, next) =>
                {
                    if (
                        context.Request.Headers["test-authenticated-tenant"].FirstOrDefault() is
                        { } tenant
                    )
                        context.User = new ClaimsPrincipal(
                            new ClaimsIdentity([new Claim("dotcelery:tenant", tenant)], "test")
                        );
                    await next(context);
                }
            );
        app.UseDotCeleryDashboard();
        app.MapDotCeleryDashboard("/celery");
        await app.StartAsync();

        var address = app
            .Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!
            .Addresses.First();

        return new TestHost(app, new HttpClient { BaseAddress = new Uri(address) });
    }

    private sealed record TestHost(WebApplication App, HttpClient Client) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await App.StopAsync();
            await App.DisposeAsync();
        }
    }
}
