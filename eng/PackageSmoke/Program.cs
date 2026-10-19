using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Client;
using DotCelery.Client.Extensions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Serialization;
using DotCelery.Generated;
using DotCelery.PackageSmoke;
using DotCelery.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Runs the flow a consumer of the packed packages follows: the task is registered through the
// source generator that DotCelery.Analyzers supplies, its payloads are serialized by a
// source-generated context, and the worker runs it end to end. Exits non-zero on failure.
var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<JsonMessageSerializerOptions>(options =>
    options.Context = SmokeJsonContext.Default
);

builder.Services.AddDotCelery(celery =>
    celery
        .UseInMemoryBroker()
        .UseInMemoryBackend()
        .RegisterTasks()
        .AddClient()
        .AddWorker(options =>
        {
            options.Concurrency = 1;
            options.Queues = ["celery"];
        })
);

builder.Logging.AddFilter("Microsoft", LogLevel.Warning);

var host = builder.Build();
await host.StartAsync();

try
{
    var client = host.Services.GetRequiredService<ICeleryClient>();
    var asyncResult = await client.SendAsync<AddTask, AddInput, AddOutput>(
        new AddInput { A = 20, B = 22 }
    );
    var result = await asyncResult.GetAsync(timeout: TimeSpan.FromSeconds(30));

    if (result.Sum != 42)
    {
        Console.Error.WriteLine($"package smoke failed: expected 42, got {result.Sum}");
        return 1;
    }

    Console.WriteLine("package smoke passed: 20 + 22 = 42");
    return 0;
}
finally
{
    await host.StopAsync();
}
