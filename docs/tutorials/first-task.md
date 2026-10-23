# Run your first task

[Tutorials](README.md) / Run your first task

In this lesson you will submit an addition task to a worker and read the answer.
Both run in one process, using the in-memory broker and result backend.

## Before you begin

Install the .NET 10 SDK and open a terminal at the root of a DotCelery checkout.
Docker is not required for this lesson. The project will live under `artifacts/`,
which the repository ignores.

## 1. Create the console project

Run these commands from the repository root:

```bash
dotnet new console --framework net10.0 --output artifacts/tutorials/FirstTask
dotnet add artifacts/tutorials/FirstTask/FirstTask.csproj reference \
  src/DotCelery.Core/DotCelery.Core.csproj \
  src/DotCelery.Client/DotCelery.Client.csproj \
  src/DotCelery.Worker/DotCelery.Worker.csproj \
  src/DotCelery.Broker.InMemory/DotCelery.Broker.InMemory.csproj \
  src/DotCelery.Backend.InMemory/DotCelery.Backend.InMemory.csproj
```

You now have a console project referencing the libraries in this checkout.

## 2. Add the task and host

Replace `artifacts/tutorials/FirstTask/Program.cs` with:

```csharp
using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Client;
using DotCelery.Client.Extensions;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using DotCelery.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.ClearProviders();
builder.Services.AddDotCelery(celery => celery
    .UseInMemoryBroker()
    .UseInMemoryBackend()
    .AddTask<AddTask>()
    .AddClient()
    .AddWorker(options =>
    {
        options.Concurrency = 1;
        options.Queues = ["celery"];
    }));

using var host = builder.Build();
await host.StartAsync();
try
{
    var client = host.Services.GetRequiredService<ICeleryClient>();
    var pending = await client.SendAsync<AddTask, AddInput, AddOutput>(
        new AddInput(20, 22));
    var result = await pending.GetAsync(timeout: TimeSpan.FromSeconds(30));
    Console.WriteLine($"Result: {result.Sum}");
}
finally
{
    await host.StopAsync();
}

internal sealed record AddInput(int A, int B);
internal sealed record AddOutput(int Sum);

internal sealed class AddTask : ITask<AddInput, AddOutput>
{
    public static string TaskName => "tutorial.add";

    public Task<AddOutput> ExecuteAsync(
        AddInput input,
        ITaskContext context,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(new AddOutput(input.A + input.B));
    }
}
```

The task has a stable name and separate input and output records. The host starts
the worker before the client submits work.

## 3. Run it

```bash
dotnet run --project artifacts/tutorials/FirstTask
```

After the build output, you should see:

```text
Result: 42
```

The client submitted a message, the worker executed `AddTask`, and the result
backend made its output available to `GetAsync`.

## 4. Change the input

Change `new AddInput(20, 22)` to `new AddInput(6, 7)` and run the same command.
The result should now be `13`. You changed the submitted data without changing
the task registration or worker.

Continue with [Explore the dashboard](dashboard-tour.md). To understand what
changes when the worker runs elsewhere, read [System architecture](../explanation/architecture.md).

Source contracts: [ITask](../../src/DotCelery.Abstractions/Abstractions/ITask.cs)
and [ICeleryClient](../../src/DotCelery.Client/ICeleryClient.cs).
