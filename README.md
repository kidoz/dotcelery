# DotCelery

[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)
[![C#](https://img.shields.io/badge/C%23-14-239120)](https://learn.microsoft.com/dotnet/csharp/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A distributed task queue for .NET 10, inspired by Python's [Celery](https://docs.celeryq.dev/). DotCelery provides asynchronous task execution, scheduling, and distributed processing with enterprise-grade reliability.

## Features

### Core Features
- **Distributed Task Execution** - Execute tasks asynchronously across multiple workers
- **Multiple Brokers** - RabbitMQ, In-Memory (Redis, Azure Service Bus, Amazon SQS planned)
- **Result Backends** - Redis, PostgreSQL, MongoDB, In-Memory (SQL Server planned)
- **Canvas Workflows** - Chain, Group, and Chord primitives for complex workflows
- **Beat Scheduler** - Periodic task scheduling with cron and interval support
- **OpenTelemetry** - Built-in observability with metrics and distributed tracing

### Enterprise Features
- **ETA/Countdown** - Schedule tasks for future execution with delayed message store
- **Task Cancellation** - Revoke running or pending tasks with real-time notifications
- **Dashboard UI** - Web-based monitoring with SignalR real-time updates
- **Rate Limiting** - Sliding window algorithm for task throttling
- **Batches** - Atomic group creation with completion callbacks
- **Saga State Machine** - Long-running business process coordination
- **Compensating Actions** - Automatic rollback when saga steps fail
- **Progress Reporting** - Real-time task progress updates during execution
- **Circuit Breaker** - Fault tolerance with automatic recovery
- **Kill Switch** - Emergency task execution control
- **Multi-Tenancy** - Tenant isolation with queue routing

### Modern C# 14
- **`field` Keyword** - Semi-auto properties with validation
- **`params ReadOnlySpan<T>`** - Zero-allocation variadic methods
- **Operators in Classes** - Fluent API support (`chain + signature`)

See `ROADMAP.md` for planned features.

## Quick Start

### Installation

```bash
dotnet add package DotCelery.Core
dotnet add package DotCelery.Client
dotnet add package DotCelery.Worker
dotnet add package DotCelery.Broker.RabbitMQ
dotnet add package DotCelery.Backend.Redis
```

### Define a Task

```csharp
using DotCelery.Core.Abstractions;

public class SendEmailTask : ITask<EmailInput, EmailResult>
{
    public static string TaskName => "email.send";

    public async Task<EmailResult> ExecuteAsync(
        EmailInput input,
        ITaskContext context,
        CancellationToken ct)
    {
        // Send email logic here
        return new EmailResult(Success: true);
    }
}

public record EmailInput(string To, string Subject, string Body);
public record EmailResult(bool Success);
```

### Configure Services

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDotCelery(celery =>
{
    celery
        .UseRabbitMQ(options =>
        {
            options.ConnectionString = "amqp://localhost";
        })
        .UseRedis(options =>
        {
            options.ConnectionString = "localhost:6379";
        })
        .AddClient()
        .AddWorker()
        .AddTasksFromAssembly(typeof(Program).Assembly);
});

var host = builder.Build();
await host.RunAsync();
```

### Send Tasks

```csharp
public class EmailService(ICeleryClient celery)
{
    public async Task<AsyncResult<EmailResult>> SendWelcomeEmailAsync(string userEmail)
    {
        var asyncResult = await celery.SendAsync<SendEmailTask, EmailInput, EmailResult>(
            new EmailInput(userEmail, "Welcome!", "Thanks for signing up."));

        return asyncResult;
    }

    public async Task<EmailResult> SendAndWaitAsync(string userEmail)
    {
        var asyncResult = await celery.SendAsync<SendEmailTask, EmailInput, EmailResult>(
            new EmailInput(userEmail, "Hello", "World"));

        var result = await asyncResult.GetAsync(timeout: TimeSpan.FromSeconds(30));

        return result;
    }
}
```

## Canvas Workflows

DotCelery supports workflow primitives for orchestrating complex task execution patterns.
These examples assume an `IMessageSerializer` named `serializer` (for example, `new JsonMessageSerializer()`).

### Chain - Sequential Execution

```csharp
var fetch = new Signature
{
    TaskName = FetchDataTask.TaskName,
    Args = serializer.Serialize(new FetchInput(url)),
};

var transform = new Signature { TaskName = TransformTask.TaskName };
var save = new Signature { TaskName = SaveTask.TaskName };

var chain = fetch.Then(transform).Then(save);
```

### Group - Parallel Execution

```csharp
var group = new Group(
    new Signature
    {
        TaskName = SendEmailTask.TaskName,
        Args = serializer.Serialize(new EmailInput("user1@example.com", "Hi", "Body")),
    },
    new Signature
    {
        TaskName = SendEmailTask.TaskName,
        Args = serializer.Serialize(new EmailInput("user2@example.com", "Hi", "Body")),
    },
    new Signature
    {
        TaskName = SendEmailTask.TaskName,
        Args = serializer.Serialize(new EmailInput("user3@example.com", "Hi", "Body")),
    }
);
```

### Chord - Parallel + Callback

```csharp
var chord = new Group(
    new Signature
    {
        TaskName = FetchPriceTask.TaskName,
        Args = serializer.Serialize(new PriceInput("AAPL")),
    },
    new Signature
    {
        TaskName = FetchPriceTask.TaskName,
        Args = serializer.Serialize(new PriceInput("GOOGL")),
    },
    new Signature
    {
        TaskName = FetchPriceTask.TaskName,
        Args = serializer.Serialize(new PriceInput("MSFT")),
    }
).WithCallback(new Signature { TaskName = AggregateTask.TaskName });
```

## Saga State Machine

Coordinate long-running business processes with automatic compensation on failure.

```csharp
var saga = new Saga
{
    Id = Guid.NewGuid().ToString(),
    Name = "OrderProcessing",
    State = SagaState.Created,
    CreatedAt = DateTimeOffset.UtcNow,
    Steps =
    [
        new SagaStep
        {
            Id = "step-1",
            Name = "Reserve Inventory",
            Order = 0,
            ExecuteTask = new Signature { TaskName = ReserveInventoryTask.TaskName, Args = orderArgs },
            CompensateTask = new Signature { TaskName = ReleaseInventoryTask.TaskName }
        },
        new SagaStep
        {
            Id = "step-2",
            Name = "Charge Payment",
            Order = 1,
            ExecuteTask = new Signature { TaskName = ChargePaymentTask.TaskName, Args = paymentArgs },
            CompensateTask = new Signature { TaskName = RefundPaymentTask.TaskName }
        },
        new SagaStep
        {
            Id = "step-3",
            Name = "Ship Order",
            Order = 2,
            ExecuteTask = new Signature { TaskName = ShipOrderTask.TaskName, Args = shippingArgs }
            // No compensation - shipping cannot be undone
        }
    ]
};

// Start the saga - if step 3 fails, steps 1 and 2 are compensated automatically
var startedSaga = await sagaOrchestrator.StartAsync(saga);
```

## Progress Reporting

Report task progress in real-time during execution.

```csharp
public class ProcessFileTask : ITask<FileInput, FileResult>
{
    public static string TaskName => "file.process";

    public async Task<FileResult> ExecuteAsync(
        FileInput input, ITaskContext context, CancellationToken ct)
    {
        var lines = await File.ReadAllLinesAsync(input.Path, ct);

        for (int i = 0; i < lines.Length; i++)
        {
            await ProcessLineAsync(lines[i], ct);

            // Report progress
            await context.Progress.ReportAsync(
                percentage: (i + 1) * 100.0 / lines.Length,
                message: $"Processing line {i + 1} of {lines.Length}");
        }

        return new FileResult { LinesProcessed = lines.Length };
    }
}
```

## Batches

Create atomic task groups with completion callbacks.

```csharp
var batchId = await batchClient.CreateBatchAsync(batch =>
{
    batch.WithName("email-campaign");
    batch.Enqueue<SendEmailTask, EmailInput>(new EmailInput("user1@example.com", "Newsletter", body));
    batch.Enqueue<SendEmailTask, EmailInput>(new EmailInput("user2@example.com", "Newsletter", body));
    batch.OnComplete<CampaignCompleteTask, CampaignCompleteInput>(
        new CampaignCompleteInput("email-campaign"));
});

var batchState = await batchClient.WaitForBatchAsync(batchId);
```

Batch state tracking requires an `IBatchStore` registration and `AddBatchSupport()` on the worker.

## Beat Scheduler

Schedule periodic tasks with cron expressions or intervals.

```csharp
using DotCelery.Core.Serialization;

var serializer = new JsonMessageSerializer();

builder.Services.AddBeatScheduler(schedule =>
{
    // Run daily at midnight
    schedule.AddCron(
        "cleanup-daily",
        "0 0 * * *",
        CleanupTask.TaskName,
        serializer.Serialize(new CleanupInput()));

    // Run every 5 minutes
    schedule.AddInterval(
        "health-check",
        TimeSpan.FromMinutes(5),
        HealthCheckTask.TaskName,
        serializer.Serialize(new HealthCheckInput()));
});
```

## Task Configuration

### Using Attributes

```csharp
[Route("high-priority")]
[RateLimit("100/m")]
[TimeLimit(SoftLimitSeconds = 300, HardLimitSeconds = 360)]
public class ProcessOrderTask : ITask<OrderInput, OrderResult>
{
    public static string TaskName => "orders.process";

    public async Task<OrderResult> ExecuteAsync(
        OrderInput input,
        ITaskContext context,
        CancellationToken ct)
    {
        try
        {
            // Process order
            return new OrderResult(Success: true);
        }
        catch (TransientException ex)
        {
            context.Retry(countdown: TimeSpan.FromSeconds(30), exception: ex);
            throw;
        }
    }
}
```

### Programmatic Retry

```csharp
public async Task<OrderResult> ExecuteAsync(
    OrderInput input,
    ITaskContext context,
    CancellationToken ct)
{
    if (context.RetryCount < context.MaxRetries)
    {
        context.Retry(countdown: TimeSpan.FromSeconds(10));
    }

    throw new InvalidOperationException("Max retries exceeded.");
}
```

## Resilience

### Circuit Breaker

Prevent cascade failures with automatic circuit breaking.

```csharp
builder.Services.AddDotCelery(celery =>
{
    celery
        .AddWorker()
        .UseCircuitBreaker(options =>
        {
            options.FailureThreshold = 5;
            options.FailureWindow = TimeSpan.FromMinutes(1);
            options.OpenDuration = TimeSpan.FromSeconds(30);
        });
});
```

### Kill Switch

Emergency control to stop task execution.

```csharp
builder.Services.AddDotCelery(celery =>
{
    celery
        .AddWorker()
        .UseKillSwitch(options =>
        {
            options.ActivationThreshold = 10;
            options.TripThreshold = 0.15;
            options.RestartTimeout = TimeSpan.FromMinutes(1);
        });
});
```

## Observability

### OpenTelemetry Integration

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics =>
    {
        metrics.AddDotCeleryInstrumentation();
    })
    .WithTracing(tracing =>
    {
        tracing.AddDotCeleryInstrumentation();
    });
```

### Available Metrics

| Metric | Description |
|--------|-------------|
| `dotcelery_tasks_sent_total` | Total tasks sent |
| `dotcelery_tasks_succeeded_total` | Total successful executions |
| `dotcelery_tasks_failed_total` | Total failed executions |
| `dotcelery_tasks_retried_total` | Total retry attempts |
| `dotcelery_task_duration_seconds` | Task execution duration |
| `dotcelery_worker_active_tasks` | Currently executing tasks |
| `dotcelery_circuit_breaker_state` | Circuit breaker state changes |
| `dotcelery_saga_completed_total` | Completed sagas |
| `dotcelery_saga_compensated_total` | Compensated sagas |

## Project Structure

```
dotcelery/
├── src/
│   ├── DotCelery.Core/              # Core abstractions, models, canvas primitives
│   ├── DotCelery.Client/            # Task sending client, batch client
│   ├── DotCelery.Worker/            # Worker, sagas, resilience, progress
│   ├── DotCelery.Beat/              # Beat scheduler for periodic tasks
│   ├── DotCelery.Cron/              # Cron expression parser
│   ├── DotCelery.Dashboard/         # Web dashboard with SignalR
│   ├── DotCelery.Broker.InMemory/   # In-memory broker (testing)
│   ├── DotCelery.Broker.RabbitMQ/   # RabbitMQ broker
│   ├── DotCelery.Backend.InMemory/  # In-memory backend (testing)
│   ├── DotCelery.Backend.Redis/     # Redis backend
│   ├── DotCelery.Backend.Postgres/  # PostgreSQL backend
│   ├── DotCelery.Backend.Mongo/     # MongoDB backend
│   └── DotCelery.Telemetry/         # OpenTelemetry instrumentation
└── tests/
    ├── DotCelery.Tests.Unit/        # Unit tests (xUnit v3)
    └── DotCelery.Tests.Integration/ # Integration tests (Testcontainers)
```

## Requirements

- .NET 10.0 or later
- C# 14

### Broker Requirements

| Broker | Version |
|--------|---------|
| RabbitMQ | 3.8+ |

### Backend Requirements

| Backend | Version |
|---------|---------|
| Redis | 6.0+ |
| PostgreSQL | 12+ |
| MongoDB | 5.0+ |

## Testing

```bash
# Run all tests
dotnet test

# Run unit tests only
dotnet test tests/DotCelery.Tests.Unit

# Run integration tests (requires Docker)
dotnet test tests/DotCelery.Tests.Integration
```

Integration tests use [Testcontainers](https://testcontainers.com/) to spin up RabbitMQ, Redis, PostgreSQL, and MongoDB containers automatically.

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting a pull request.

```bash
git clone https://github.com/dotcelery/dotcelery.git
cd dotcelery
dotnet restore
dotnet build
dotnet test tests/DotCelery.Tests.Unit
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by [Celery](https://docs.celeryq.dev/) for Python
- Built with [.NET 10](https://dotnet.microsoft.com/) and [C# 14](https://learn.microsoft.com/dotnet/csharp/)
