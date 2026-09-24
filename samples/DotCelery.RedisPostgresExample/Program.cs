using DotCelery.Backend.Postgres.Extensions;
using DotCelery.Backend.Redis.Extensions;
using DotCelery.Broker.Redis.Extensions;
using DotCelery.Client;
using DotCelery.Client.Extensions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Models;
using DotCelery.RedisPostgresExample;
using DotCelery.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var backendName = GetBackendName(args);

var builder = Host.CreateApplicationBuilder(args);
builder.Environment.EnvironmentName = Environments.Development;

var redisConnectionString =
    builder.Configuration["DOTCELERY_REDIS_CONNECTION_STRING"] ?? "localhost:6380";
var postgresConnectionString =
    builder.Configuration["DOTCELERY_POSTGRES_CONNECTION_STRING"]
    ?? "Host=localhost;Port=5433;Database=dotcelery;Username=dotcelery;Password=dotcelery";
var redisStreamKeyPrefix = $"dotcelery:example:{Environment.ProcessId}:stream:";

builder.Services.AddDotCelery(celery =>
{
    celery
        .UseRedisBroker(options =>
        {
            options.ConnectionString = redisConnectionString;
            options.StreamKeyPrefix = redisStreamKeyPrefix;
            options.BlockTimeout = TimeSpan.FromMilliseconds(250);
        })
        .AddTask<ResizeImageTask>()
        .AddClient(options =>
        {
            options.DefaultTimeout = TimeSpan.FromSeconds(30);
        })
        .AddWorker(options =>
        {
            options.Concurrency = 1;
            options.Queues = ["media"];
        });

    if (backendName == "redis")
    {
        celery.UseRedis(options =>
        {
            options.ConnectionString = redisConnectionString;
            options.KeyPrefix = "dotcelery:example:result:";
            options.StateKeyPrefix = "dotcelery:example:state:";
        });
    }
    else
    {
        celery.UsePostgres(options =>
        {
            options.ConnectionString = postgresConnectionString;
            options.Schema = "public";
            options.TableName = "dotcelery_example_task_results";
            options.UseListenNotify = true;
        });
    }
});

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
});
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddFilter("DotCelery", LogLevel.Information);

using var host = builder.Build();

Console.WriteLine("DotCelery Redis/Postgres example");
Console.WriteLine($"Redis broker: {redisConnectionString}");
Console.WriteLine($"Result backend: {backendName}");

await host.StartAsync();

try
{
    var client = host.Services.GetRequiredService<ICeleryClient>();

    var input = new ResizeImageInput(
        SourceUri: "s3://media/originals/banner.png",
        Width: 1280,
        Height: 720
    );

    var asyncResult = await client.SendAsync<ResizeImageTask, ResizeImageInput, ResizeImageResult>(
        input,
        new SendOptions { Queue = "media" }
    );

    Console.WriteLine($"Sent task {asyncResult.TaskId}");

    await WaitUntilCompleteAsync(asyncResult, TimeSpan.FromSeconds(30));
    var result = await asyncResult.GetAsync(timeout: TimeSpan.FromSeconds(30));

    Console.WriteLine($"Stored result: {result.OutputUri}");
    Console.WriteLine($"Dimensions: {result.Width}x{result.Height}");
}
finally
{
    await host.StopAsync();
}

static string GetBackendName(string[] args)
{
    const string prefix = "--result-backend=";
    var arg = args.FirstOrDefault(a => a.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));

    if (arg is null)
    {
        return "postgres";
    }

    var backend = arg[prefix.Length..].Trim().ToLowerInvariant();
    return backend == "redis" ? "redis" : "postgres";
}

static async Task WaitUntilCompleteAsync(AsyncResult asyncResult, TimeSpan timeout)
{
    using var cts = new CancellationTokenSource(timeout);

    while (!cts.IsCancellationRequested)
    {
        var state = await asyncResult.GetStateAsync(cts.Token);
        if (
            state
            is TaskState.Success
                or TaskState.Failure
                or TaskState.Revoked
                or TaskState.Rejected
        )
        {
            return;
        }

        await Task.Delay(TimeSpan.FromMilliseconds(100), cts.Token);
    }

    throw new TimeoutException(
        $"Timed out waiting for task {asyncResult.TaskId} to complete after {timeout}."
    );
}
