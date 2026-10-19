using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.CanvasExample.Tasks;
using DotCelery.Client;
using DotCelery.Client.Canvas;
using DotCelery.Client.Extensions;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Canvas;
using DotCelery.Core.Extensions;
using DotCelery.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

Console.WriteLine("===========================================");
Console.WriteLine("     DotCelery Canvas Workflow Example");
Console.WriteLine("===========================================");
Console.WriteLine();

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDotCelery(celery =>
    celery
        .UseInMemoryBroker()
        .UseInMemoryBackend()
        // The batch store tracks a chord's header tasks and dispatches its callback
        .AddInMemoryBatchStore()
        .AddTask<GenerateNumbersTask>()
        .AddTask<SumNumbersTask>()
        .AddTask<FormatReportTask>()
        .AddTask<SquareNumberTask>()
        .AddTask<AggregateResultsTask>()
        .AddClient()
        .AddCanvasClient()
        // Signal handlers that settle batch and chord headers
        .AddBatchSupport()
        .AddWorker(options =>
        {
            options.Concurrency = 4;
            options.Queues = ["celery"];
        })
);

builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
});
builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddFilter("DotCelery", LogLevel.Warning);
builder.Logging.AddFilter("DotCelery.CanvasExample", LogLevel.Information);

using var host = builder.Build();

await host.StartAsync();

try
{
    var canvasClient = host.Services.GetRequiredService<ICanvasClient>();
    var client = host.Services.GetRequiredService<ICeleryClient>();
    var serializer = host.Services.GetRequiredService<IMessageSerializer>();

    await RunChainAsync(canvasClient, client, serializer);
    await RunGroupAsync(canvasClient, client, serializer);
    await RunChordAsync(canvasClient, client, serializer);
    await RunNestedGroupAsync(canvasClient, client, serializer);

    Console.WriteLine("Canvas example passed.");
    return 0;
}
finally
{
    await host.StopAsync();
}

static async Task RunChainAsync(
    ICanvasClient canvasClient,
    ICeleryClient client,
    IMessageSerializer serializer
)
{
    Console.WriteLine("--- Chain: each step runs after the previous one succeeds ---");

    var chain = new Signature<GenerateNumbersTask, GenerateNumbersInput, NumbersResult>
    {
        Input = new GenerateNumbersInput { Count = 6, Start = 5 },
    }
        .Then(new Signature<SumNumbersTask, NumbersResult, SumResult>())
        .Then(new Signature<FormatReportTask, SumResult, ReportResult>());

    var chainResult = await canvasClient.SendChainAsync(chain);
    Console.WriteLine($"  Chain {chainResult.Id}");
    Console.WriteLine($"  Task IDs: {string.Join(", ", chainResult.TaskIds)}");

    var report = await new AsyncResult<ReportResult>(
        chainResult.LastTaskId,
        client,
        serializer
    ).GetAsync(timeout: TimeSpan.FromSeconds(30));

    Ensure(report.Text == "Sum of 6 values = 45", $"Unexpected chain report: {report.Text}");
    Console.WriteLine($"  Result: {report.Text}");
    Console.WriteLine();
}

static async Task RunGroupAsync(
    ICanvasClient canvasClient,
    ICeleryClient client,
    IMessageSerializer serializer
)
{
    Console.WriteLine("--- Group: every member runs in parallel ---");

    var group = new Group(Square(2, 400), Square(3, 400), Square(4, 400), Square(5, 400));

    var groupResult = await canvasClient.SendGroupAsync(group);
    Console.WriteLine($"  Group {groupResult.Id} with {groupResult.Count} members");

    var results = await Task.WhenAll(
        groupResult.TaskIds.Select(taskId =>
            new AsyncResult<SquareResult>(taskId, client, serializer).GetAsync(
                timeout: TimeSpan.FromSeconds(30)
            )
        )
    );

    Ensure(
        results.Select(result => result.Square).SequenceEqual([4, 9, 16, 25]),
        "Unexpected group results"
    );

    foreach (var result in results)
    {
        Console.WriteLine(
            $"  {result.Value}^2 = {result.Square} "
                + $"({result.StartedAt:HH:mm:ss.fff} -> {result.CompletedAt:HH:mm:ss.fff})"
        );
    }

    Console.WriteLine();
}

static async Task RunChordAsync(
    ICanvasClient canvasClient,
    ICeleryClient client,
    IMessageSerializer serializer
)
{
    Console.WriteLine("--- Chord: a callback runs once every header task finished ---");

    var header = new Group(Square(7, 300), Square(8, 300), Square(9, 300));
    var callback = new Signature<AggregateResultsTask, AggregateInput, AggregateResult>
    {
        Input = new AggregateInput { Label = "squares", ExpectedCount = 3 },
    };

    var chordResult = await canvasClient.SendChordAsync(header.WithCallback(callback));
    Console.WriteLine($"  Chord {chordResult.Id} with {chordResult.Header.Count} header tasks");

    var aggregate = await new AsyncResult<AggregateResult>(
        chordResult.CallbackTaskId,
        client,
        serializer
    ).GetAsync(timeout: TimeSpan.FromSeconds(30));

    var headerResults = await Task.WhenAll(
        chordResult.Header.TaskIds.Select(taskId =>
            new AsyncResult<SquareResult>(taskId, client, serializer).GetAsync(
                timeout: TimeSpan.FromSeconds(30)
            )
        )
    );

    Ensure(aggregate.ExpectedCount == headerResults.Length, "Unexpected callback input");
    Ensure(
        aggregate.CompletedAt >= headerResults.Max(result => result.CompletedAt),
        "The callback ran before the header tasks finished"
    );

    Console.WriteLine(
        $"  Header results: {string.Join(", ", headerResults.Select(result => $"{result.Value}^2 = {result.Square}"))}"
    );
    Console.WriteLine(
        $"  Callback '{aggregate.Label}' completed at {aggregate.CompletedAt:HH:mm:ss.fff}"
    );
    Console.WriteLine();
}

static async Task RunNestedGroupAsync(
    ICanvasClient canvasClient,
    ICeleryClient client,
    IMessageSerializer serializer
)
{
    Console.WriteLine("--- Nested canvas: a group of chains and a signature ---");

    // A chain member completes with its last step, so these two chains and the signature are the
    // group's three completion IDs, in member order
    var nested = new Group(
        ReportChain(count: 3, start: 1),
        ReportChain(count: 4, start: 10),
        Square(12, 100)
    );

    var groupResult = await canvasClient.SendGroupAsync(nested);
    Console.WriteLine($"  Group {groupResult.Id} with {groupResult.Count} completion IDs");

    var firstReport = await new AsyncResult<ReportResult>(
        groupResult.TaskIds[0],
        client,
        serializer
    ).GetAsync(timeout: TimeSpan.FromSeconds(30));
    var secondReport = await new AsyncResult<ReportResult>(
        groupResult.TaskIds[1],
        client,
        serializer
    ).GetAsync(timeout: TimeSpan.FromSeconds(30));
    var square = await new AsyncResult<SquareResult>(
        groupResult.TaskIds[2],
        client,
        serializer
    ).GetAsync(timeout: TimeSpan.FromSeconds(30));

    Ensure(firstReport.Text == "Sum of 3 values = 6", $"Unexpected report: {firstReport.Text}");
    Ensure(secondReport.Text == "Sum of 4 values = 46", $"Unexpected report: {secondReport.Text}");
    Ensure(square.Square == 144, $"Unexpected square: {square.Square}");

    Console.WriteLine($"  Chain 1: {firstReport.Text}");
    Console.WriteLine($"  Chain 2: {secondReport.Text}");
    Console.WriteLine($"  Signature: 12^2 = {square.Square}");
    Console.WriteLine();
}

static Signature<SquareNumberTask, SquareInput, SquareResult> Square(int value, int delayMs) =>
    new()
    {
        Input = new SquareInput { Value = value, DelayMs = delayMs },
    };

static Chain ReportChain(int count, int start) =>
    new Signature<GenerateNumbersTask, GenerateNumbersInput, NumbersResult>
    {
        Input = new GenerateNumbersInput { Count = count, Start = start },
    }
        .Then(new Signature<SumNumbersTask, NumbersResult, SumResult>())
        .Then(new Signature<FormatReportTask, SumResult, ReportResult>());

static void Ensure(bool condition, string message)
{
    if (!condition)
    {
        throw new InvalidOperationException(message);
    }
}
