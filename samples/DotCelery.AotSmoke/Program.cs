using DotCelery.AotSmoke;
using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Client;
using DotCelery.Client.Extensions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Serialization;
using DotCelery.Generated;
using DotCelery.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// A Native AOT smoke test: the task is registered by the source generator, the payload types are
// serialized by a source-generated JSON context, and the task is invoked through the generated
// adapter, so no reflection or expression compilation happens at runtime. Exits non-zero when the
// round trip does not produce the expected result.
var builder = Host.CreateApplicationBuilder(args);

// Every application payload type is registered in the context, so JSON serialization needs no
// reflection; DotCelery's own model types come from its built-in context
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

using var host = builder.Build();
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
        Console.Error.WriteLine($"aot smoke failed: expected 42, got {result.Sum}");
        return 1;
    }

    Console.WriteLine("aot smoke passed: 20 + 22 = 42");
    return 0;
}
finally
{
    await host.StopAsync();
}
