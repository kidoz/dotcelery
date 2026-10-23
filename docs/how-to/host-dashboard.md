# Host and authorize the dashboard

[How-to guides](README.md) / Host the dashboard

Add the dashboard to an ASP.NET Core host that already has DotCelery services and
an application authentication scheme configured. Install or reference
`DotCelery.Dashboard`. Building its source requires Node.js 24.3 or later;
consuming the compiled NuGet package does not.

## Register one prefix and an authorization policy

Before `builder.Build()`, add:

```csharp
using DotCelery.Dashboard.Extensions;

const string dashboardPrefix = "/jobs/monitor";

builder.Services.AddDotCeleryDashboard(options =>
{
    options.PathPrefix = dashboardPrefix;
    options.Title = "Operations";
    options.RequireAuthorization = true;
    options.AuthorizationCallback = context => Task.FromResult(
        context.User.Identity?.IsAuthenticated == true
        && context.User.IsInRole("Operations"));
    options.ReadOnly = true;
});
```

This example grants access to authenticated members of the host's `Operations`
role and starts with task mutations disabled. With the default
`RequireAuthorization = true`, a missing callback denies access.

## Map the UI, API, and hub

Use the same prefix when mapping endpoints. Put the host's existing authentication
and authorization middleware before the dashboard middleware:

```csharp
var app = builder.Build();
app.UseAuthentication();
app.UseAuthorization();
app.UseDotCeleryDashboard();
app.MapDotCeleryDashboard(dashboardPrefix);
app.Run();
```

The UI is at `/jobs/monitor/`, the REST API is under `/jobs/monitor/api/v1`, and the
hub is at `/jobs/monitor/hub`. Verify that an authorized user can open the page and
that a user rejected by your callback receives 403.

## Connect the data you need

- Keep the worker and dashboard in the same process to use the default worker
  registry, or register a shared `AddRedisWorkerRegistry`,
  `AddPostgresWorkerRegistry`, `AddSqlServerWorkerRegistry`, or
  `AddMongoWorkerRegistry` on the storage used by your worker hosts.
- Register task storage for retained history and queue metrics for queue gauges.
  The [demo setup](../../samples/DotCelery.Dashboard.Demo/Program.cs) shows the
  in-memory registrations together.
- Register a historical data store to populate the completion chart. Without one,
  the UI reports that historical metrics are unavailable.

## Enable task operations when required

Set `ReadOnly = false` and keep `AllowTaskOperations = true`. Cancellation also
needs a revocation store; retry needs a dead-letter store containing the original
message. Check `/jobs/monitor/api/v1/capabilities` to confirm which operations the
host actually supports.

State-changing requests must retain the dashboard verification header and the
host's origin checks. A cancellation response records acceptance of a request;
it does not prove that a running task stopped. See
[Dashboard API v1](../reference/dashboard-api.md) for the operation contracts and
[Dashboard options](../reference/dashboard-options.md) for all settings.

Versioning uses the host's shared MVC services. Mark unrelated unversioned MVC
API controllers with `[ApiVersionNeutral]`, or declare their API versions. See
[versioning integration](../reference/dashboard-api.md#versioning-and-discovery).
