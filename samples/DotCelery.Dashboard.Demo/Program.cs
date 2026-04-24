using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Backend.InMemory.Historical;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Client.Extensions;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using DotCelery.Dashboard.Demo.HostedServices;
using DotCelery.Dashboard.Demo.Tasks;
using DotCelery.Dashboard.Extensions;
using DotCelery.Worker.Extensions;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddFilter("DotCelery", LogLevel.Information);

// 1. Wire DotCelery: in-memory broker + backend, register tasks, run a worker in-process.
builder.Services.AddDotCelery(celery =>
    celery
        .UseInMemoryBroker()
        .UseInMemoryBackend()
        .AddInMemoryHighPriorityFeatures() // delayed store, revocation, rate limiter
        .AddTask<EmailTask>()
        .AddTask<CalculationTask>()
        .AddTask<FlakyTask>()
        .AddClient()
        .AddWorker(o =>
        {
            o.WorkerName = "demo-worker-1";
            o.Concurrency = 4;
            o.Queues = ["celery"];
        })
        .AddDelayedMessageDispatcher()
);

// 2. Optional: historical metrics backing store powers the /metrics/historical endpoints.
builder.Services.AddSingleton<IHistoricalDataStore, InMemoryHistoricalDataStore>();

// 3. Mount the dashboard. Default prefix is "/celery".
builder.Services.AddDotCeleryDashboard(o =>
{
    o.Title = "DotCelery Demo Dashboard";
    o.PathPrefix = "/celery";
    o.RefreshIntervalSeconds = 3;
    o.AllowTaskOperations = true; // show Revoke buttons
    o.ExposeExceptionDetails = true; // demo only — keep false in prod

    // The dashboard is fail-secure by default. For the demo we open it up;
    // in production wire this to your auth (cookie/JWT/role check).
    o.RequireAuthorization = false;
});

// 4. Hosted services that make the dashboard interesting:
//    - register the in-process worker so the Workers panel is populated
//    - keep enqueueing tasks so the Recent Tasks panel keeps moving
builder.Services.AddHostedService<WorkerHeartbeatService>();
builder.Services.AddHostedService<TrafficGeneratorService>();

var app = builder.Build();

// The dashboard middleware is a terminal handler for everything under its path
// prefix and would otherwise shadow the API controllers (e.g. /celery/api/overview
// would return index.html). Run it only when routing did NOT match an endpoint —
// API controllers and the SignalR hub then win for their routes.
app.UseRouting();
app.UseWhen(ctx => ctx.GetEndpoint() is null, branch => branch.UseDotCeleryDashboard());

app.MapDotCeleryDashboard("/celery"); // API controllers + SignalR hub
app.MapGet("/", () => Results.Redirect("/celery/"));

app.Run();
