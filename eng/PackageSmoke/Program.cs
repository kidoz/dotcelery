using System.Text;
using System.Text.RegularExpressions;
using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Client;
using DotCelery.Client.Extensions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Serialization;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Middleware;
using DotCelery.Generated;
using DotCelery.PackageSmoke;
using DotCelery.Worker.Extensions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

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

    await VerifyDashboardAsync();
    Console.WriteLine("package smoke passed: 20 + 22 = 42; embedded dashboard assets are served");
    return 0;
}
finally
{
    await host.StopAsync();
}

static async Task VerifyDashboardAsync()
{
    var middleware = new DashboardMiddleware(
        _ => Task.CompletedTask,
        Options.Create(
            new DashboardOptions { RequireAuthorization = false, PathPrefix = "/jobs/monitor" }
        )
    );
    var context = new DefaultHttpContext();
    context.Request.Path = "/jobs/monitor/deep/route";
    await using var body = new MemoryStream();
    context.Response.Body = body;
    await middleware.InvokeAsync(context);
    var html = Encoding.UTF8.GetString(body.ToArray());
    if (!html.Contains("<base href=\"/jobs/monitor/\"", StringComparison.Ordinal))
        throw new InvalidOperationException(
            "The packaged dashboard page is missing its runtime base path."
        );

    var assets = Regex.Matches(
        html,
        "(?:src|href)=\"\\./(assets/[^\"]+)\"",
        RegexOptions.NonBacktracking
    );
    if (assets.Count < 2)
        throw new InvalidOperationException(
            "The packaged dashboard page is missing its built scripts or stylesheet."
        );
    foreach (Match asset in assets)
    {
        var request = new DefaultHttpContext();
        request.Request.Path = "/jobs/monitor/" + asset.Groups[1].Value;
        await using var content = new MemoryStream();
        request.Response.Body = content;
        await middleware.InvokeAsync(request);
        if (request.Response.StatusCode != StatusCodes.Status200OK || content.Length == 0)
            throw new InvalidOperationException(
                "A built dashboard asset is missing from the NuGet package."
            );
    }
}
