using DotCelery.Backend.InMemory.Extensions;
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
        .AddInMemoryStorage() // task records for the dashboard's task list
        .AddInMemoryDelayedMessageStore()
        .AddInMemoryRevocationStore()
        .AddInMemoryDeadLetterStore()
        .AddInMemoryRateLimiter()
        .AddInMemoryQueueMetrics() // queue statistics for the dashboard
        .AddInMemoryHistoricalDataStore() // powers the metrics panel and /metrics/historical
        .AddTask<EmailTask>()
        .AddTask<CalculationTask>()
        .AddTask<FlakyTask>()
        .AddClient()
        .AddWorker(o =>
        {
            o.WorkerName = "demo-worker-1";
            o.Concurrency = 4;
            o.Queues = ["celery"];
            o.WorkerHeartbeatInterval = TimeSpan.FromSeconds(5);
        })
        .AddDelayedMessageDispatcher()
        .UseQueueMetrics() // records the queue statistics the dashboard shows
);

// 2. Mount the dashboard. Default prefix is "/celery".
builder.Services.AddDotCeleryDashboard(o =>
{
    o.Title = "DotCelery Demo Dashboard";
    o.PathPrefix = "/celery";
    o.RefreshIntervalSeconds = 3;
    o.MetricsSnapshotInterval = TimeSpan.FromSeconds(15); // demo only — the default is a minute
    o.AllowTaskOperations = true; // capability-aware cancellation and retry
    o.ReadOnly = builder.Configuration.GetValue<bool>("Dashboard:ReadOnly");
    o.ExposeExceptionDetails = true; // demo only — keep false in prod

    // The dashboard is fail-secure by default. For the demo we open it up;
    // in production wire this to your auth (cookie/JWT/role check).
    o.RequireAuthorization = false;
});

// 3. Keep enqueueing tasks so the Recent Tasks panel keeps moving. The worker registers
//    itself with the dashboard's registry (the heartbeat service AddWorker adds), so the
//    Workers panel is populated without any wiring here.
builder.Services.AddHostedService<TrafficGeneratorService>();

var app = builder.Build();

app.UseDotCeleryDashboard(); // the UI page; API and hub paths pass through to their endpoints
app.MapDotCeleryDashboard("/celery"); // API controllers + SignalR hub
app.MapGet("/", () => Results.Redirect("/celery/"));

app.Run();
