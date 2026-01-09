using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Client;
using DotCelery.Client.Extensions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Models;
using DotCelery.Demo.Tasks;
using DotCelery.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

Console.WriteLine("===========================================");
Console.WriteLine("       DotCelery Demo Application");
Console.WriteLine("===========================================");
Console.WriteLine();

// Build the host with DotCelery services
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDotCelery(celery =>
    celery
        .UseInMemoryBroker()
        .UseInMemoryBackend()
        .AddInMemoryHighPriorityFeatures()
        .AddTask<EmailTask>()
        .AddTask<CalculationTask>()
        .AddTask<LongRunningTask>()
        .AddClient()
        .AddWorker(options =>
        {
            options.Concurrency = 2;
            options.Queues = ["celery"];
        })
        .AddDelayedMessageDispatcher()
);

builder.Logging.AddFilter("Microsoft", LogLevel.Warning);
builder.Logging.AddFilter("DotCelery", LogLevel.Information);

var host = builder.Build();

// Start the host in the background
var hostTask = host.StartAsync();

// Give the worker time to start
await Task.Delay(500);

var client = host.Services.GetRequiredService<ICeleryClient>();

Console.WriteLine("Worker started. Ready to process tasks.");
Console.WriteLine();

var running = true;
while (running)
{
    Console.WriteLine("Choose a demo option:");
    Console.WriteLine("  1. Send Email Task");
    Console.WriteLine("  2. Send Calculation Task");
    Console.WriteLine("  3. Send Delayed Task (ETA/Countdown)");
    Console.WriteLine("  4. Send Long Running Task (with cancellation)");
    Console.WriteLine("  5. Send Multiple Tasks (parallel execution)");
    Console.WriteLine("  6. Exit");
    Console.WriteLine();
    Console.Write("Enter your choice (1-6): ");

    var choice = Console.ReadLine()?.Trim();

    Console.WriteLine();

    switch (choice)
    {
        case "1":
            await DemoEmailTask(client);
            break;
        case "2":
            await DemoCalculationTask(client);
            break;
        case "3":
            await DemoDelayedTask(client);
            break;
        case "4":
            await DemoLongRunningTask(client);
            break;
        case "5":
            await DemoParallelTasks(client);
            break;
        case "6":
            running = false;
            break;
        default:
            Console.WriteLine("Invalid choice. Please try again.");
            break;
    }

    Console.WriteLine();
}

Console.WriteLine("Shutting down...");
await host.StopAsync();

static async Task DemoEmailTask(ICeleryClient client)
{
    Console.WriteLine("--- Email Task Demo ---");

    var input = new EmailInput
    {
        To = "user@example.com",
        Subject = "Hello from DotCelery!",
        Body = "This is a test email sent via DotCelery task queue.",
    };

    Console.WriteLine($"Sending email task to: {input.To}");

    var asyncResult = await client.SendAsync<EmailTask, EmailInput, EmailResult>(input);
    Console.WriteLine($"Task ID: {asyncResult.TaskId}");
    Console.WriteLine("Waiting for result...");

    var result = await asyncResult.GetAsync(timeout: TimeSpan.FromSeconds(30));

    Console.WriteLine(
        $"Result: MessageId={result.MessageId}, Success={result.Success}, SentAt={result.SentAt}"
    );
}

static async Task DemoCalculationTask(ICeleryClient client)
{
    Console.WriteLine("--- Calculation Task Demo ---");

    var operations = new[]
    {
        ("add", 10, 5),
        ("subtract", 20, 8),
        ("multiply", 7, 6),
        ("divide", 100, 4),
    };

    foreach (var (op, a, b) in operations)
    {
        var input = new CalculationInput
        {
            A = a,
            B = b,
            Operation = op,
        };

        var asyncResult = await client.SendAsync<
            CalculationTask,
            CalculationInput,
            CalculationResult
        >(input);
        var result = await asyncResult.GetAsync(timeout: TimeSpan.FromSeconds(10));

        Console.WriteLine($"  {result.Expression}");
    }
}

static async Task DemoDelayedTask(ICeleryClient client)
{
    Console.WriteLine("--- Delayed Task Demo (Countdown) ---");

    var input = new EmailInput
    {
        To = "delayed@example.com",
        Subject = "Delayed Message",
        Body = "This message was sent with a 3-second delay.",
    };

    var options = new SendOptions { Countdown = TimeSpan.FromSeconds(3) };

    Console.WriteLine("Sending task with 3-second countdown...");
    var sendTime = DateTimeOffset.UtcNow;

    var asyncResult = await client.SendAsync<EmailTask, EmailInput, EmailResult>(input, options);
    Console.WriteLine($"Task ID: {asyncResult.TaskId}");
    Console.WriteLine("Waiting for delayed execution...");

    var result = await asyncResult.GetAsync(timeout: TimeSpan.FromSeconds(30));
    var delay = result.SentAt - sendTime;

    Console.WriteLine($"Task completed. Actual delay: {delay.TotalSeconds:F1} seconds");
}

static async Task DemoLongRunningTask(ICeleryClient client)
{
    Console.WriteLine("--- Long Running Task Demo (with Cancellation) ---");

    var input = new LongRunningInput { Iterations = 10, DelayPerIterationMs = 500 };

    Console.WriteLine($"Starting long running task ({input.Iterations} iterations)...");
    Console.WriteLine("Press 'C' within 3 seconds to cancel the task, or wait for completion.");

    var asyncResult = await client.SendAsync<LongRunningTask, LongRunningInput, LongRunningResult>(
        input
    );
    Console.WriteLine($"Task ID: {asyncResult.TaskId}");

    // Wait for potential cancellation input
    var cts = new CancellationTokenSource();
    var checkInputTask = Task.Run(async () =>
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(3);
        while (DateTimeOffset.UtcNow < deadline && !cts.Token.IsCancellationRequested)
        {
            if (Console.KeyAvailable && Console.ReadKey(intercept: true).Key == ConsoleKey.C)
            {
                Console.WriteLine();
                Console.WriteLine("Cancellation requested. Revoking task...");
                await client.RevokeAsync(asyncResult.TaskId);
                Console.WriteLine("Task revoked.");
                return true;
            }
            await Task.Delay(100);
        }
        return false;
    });

    var cancelled = await checkInputTask;
    cts.Cancel();

    if (cancelled)
    {
        Console.WriteLine("Task was cancelled.");
    }
    else
    {
        Console.WriteLine("Waiting for task completion...");
        try
        {
            var result = await asyncResult.GetAsync(timeout: TimeSpan.FromSeconds(30));
            Console.WriteLine(
                $"Task completed: {result.CompletedIterations} iterations in {result.TotalDuration.TotalSeconds:F1}s"
            );
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Task failed or was cancelled: {ex.Message}");
        }
    }
}

static async Task DemoParallelTasks(ICeleryClient client)
{
    Console.WriteLine("--- Parallel Tasks Demo ---");

    var tasks = new List<AsyncResult<EmailResult>>();

    Console.WriteLine("Sending 5 email tasks in parallel...");

    for (var i = 1; i <= 5; i++)
    {
        var input = new EmailInput
        {
            To = $"user{i}@example.com",
            Subject = $"Parallel Test #{i}",
            Body = $"This is parallel task number {i}.",
        };

        var asyncResult = await client.SendAsync<EmailTask, EmailInput, EmailResult>(input);
        tasks.Add(asyncResult);
        Console.WriteLine($"  Sent task {i}: {asyncResult.TaskId}");
    }

    Console.WriteLine();
    Console.WriteLine("Waiting for all tasks to complete...");

    var results = await Task.WhenAll(
        tasks.Select(t => t.GetAsync(timeout: TimeSpan.FromSeconds(30)))
    );

    Console.WriteLine();
    Console.WriteLine("All tasks completed:");
    for (var i = 0; i < results.Length; i++)
    {
        Console.WriteLine($"  Task {i + 1}: MessageId={results[i].MessageId}");
    }
}
