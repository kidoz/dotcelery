namespace DotCelery.Tests.Unit.Worker;

using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Models;
using DotCelery.Core.Resilience;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Resilience;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

public class AutoRetryTests : IAsyncDisposable
{
    private readonly InMemoryStorageProvider _storage = new();
    private readonly ResultBackend _backend;
    private readonly JsonMessageSerializer _serializer = new();
    private readonly ServiceProvider _serviceProvider;
    private readonly RevocationManager _revocationManager;
    private readonly TaskFilterPipeline _filterPipeline;

    public AutoRetryTests()
    {
        _backend = new ResultBackend(_storage);

        var services = new ServiceCollection();
        services.AddTransient<ThrowingTask>();
        services.AddTransient<RetryingTask>();
        services.AddTransient<RetryingWithCountdownTask>();

        _serviceProvider = services.BuildServiceProvider();

        var workerOptions = Options.Create(
            new WorkerOptions { EnableRevocation = false, EnableRateLimiting = false }
        );

        _revocationManager = new RevocationManager(
            workerOptions,
            NullLogger<RevocationManager>.Instance
        );

        _filterPipeline = new TaskFilterPipeline(
            _serviceProvider,
            Options.Create(new TaskFilterOptions()),
            NullLogger<TaskFilterPipeline>.Instance
        );
    }

    [Fact]
    public async Task ExecuteAsync_AutoRetry_FailureBecomesARetryWithTheComputedDelay()
    {
        var executor = CreateExecutor(
            typeof(ThrowingTask),
            ThrowingTask.TaskName,
            new ResilienceOptions
            {
                DefaultRetryPolicy = RetryPolicy.Constant(
                    delay: TimeSpan.FromSeconds(5),
                    jitter: false
                ),
                AutoRetryOnFailure = true,
            }
        );
        var message = CreateBrokerMessage("auto-retry", ThrowingTask.TaskName);

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Retry, result.State);
        Assert.Equal(TimeSpan.FromSeconds(5), result.RetryAfter);
        Assert.NotNull(result.Exception);
    }

    [Fact]
    public async Task ExecuteAsync_AutoRetry_NonRetryableExceptionStaysAFailure()
    {
        var executor = CreateExecutor(
            typeof(ThrowingTask),
            ThrowingTask.TaskName,
            new ResilienceOptions
            {
                DefaultRetryPolicy = RetryPolicy.Default() with
                {
                    NonRetryableExceptions = [typeof(InvalidOperationException)],
                },
                AutoRetryOnFailure = true,
            }
        );
        var message = CreateBrokerMessage("auto-retry-non-retryable", ThrowingTask.TaskName);

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Failure, result.State);
    }

    [Fact]
    public async Task ExecuteAsync_AutoRetry_ExhaustedRetriesStayAFailure()
    {
        var executor = CreateExecutor(
            typeof(ThrowingTask),
            ThrowingTask.TaskName,
            new ResilienceOptions
            {
                DefaultRetryPolicy = RetryPolicy.Constant(
                    maxRetries: 3,
                    delay: TimeSpan.FromSeconds(5),
                    jitter: false
                ),
                AutoRetryOnFailure = true,
            }
        );
        var message = CreateBrokerMessage(
            "auto-retry-exhausted",
            ThrowingTask.TaskName,
            retries: 3
        );

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Failure, result.State);
    }

    [Fact]
    public async Task ExecuteAsync_AutoRetryDisabled_FailureIsUnchanged()
    {
        var executor = CreateExecutor(
            typeof(ThrowingTask),
            ThrowingTask.TaskName,
            new ResilienceOptions
            {
                DefaultRetryPolicy = RetryPolicy.Constant(
                    delay: TimeSpan.FromSeconds(5),
                    jitter: false
                ),
                AutoRetryOnFailure = false,
            }
        );
        var message = CreateBrokerMessage("auto-retry-disabled", ThrowingTask.TaskName);

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Failure, result.State);
        Assert.Null(result.RetryAfter);
    }

    [Fact]
    public async Task ExecuteAsync_RequestedRetryWithoutCountdown_UsesThePolicyBackoff()
    {
        var executor = CreateExecutor(
            typeof(RetryingTask),
            RetryingTask.TaskName,
            new ResilienceOptions
            {
                DefaultRetryPolicy = RetryPolicy.Constant(
                    delay: TimeSpan.FromSeconds(7),
                    jitter: false
                ),
            }
        );
        var message = CreateBrokerMessage("retry-no-countdown", RetryingTask.TaskName);

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Retry, result.State);
        Assert.Equal(TimeSpan.FromSeconds(7), result.RetryAfter);
    }

    [Fact]
    public async Task ExecuteAsync_RequestedRetryWithoutCountdownOrPolicy_RepublishesImmediately()
    {
        var executor = CreateExecutor(
            typeof(RetryingTask),
            RetryingTask.TaskName,
            new ResilienceOptions()
        );
        var message = CreateBrokerMessage("retry-immediate", RetryingTask.TaskName);

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Retry, result.State);
        Assert.Null(result.RetryAfter);
    }

    [Fact]
    public async Task ExecuteAsync_RequestedRetryWithCountdown_CountdownWinsOverThePolicy()
    {
        var executor = CreateExecutor(
            typeof(RetryingWithCountdownTask),
            RetryingWithCountdownTask.TaskName,
            new ResilienceOptions
            {
                DefaultRetryPolicy = RetryPolicy.Constant(
                    delay: TimeSpan.FromSeconds(7),
                    jitter: false
                ),
            }
        );
        var message = CreateBrokerMessage(
            "retry-with-countdown",
            RetryingWithCountdownTask.TaskName
        );

        var result = await executor.ExecuteAsync(message, "worker-1", CancellationToken.None);

        Assert.Equal(TaskState.Retry, result.State);
        Assert.Equal(TimeSpan.FromSeconds(30), result.RetryAfter);
    }

    [Fact]
    public void MaxRetriesLimit_ClampsThePublisherSuppliedMaxRetries()
    {
        var message = CreateBrokerMessage("clamped", ThrowingTask.TaskName).Message with
        {
            MaxRetries = int.MaxValue,
        };

        var context = new TaskExecutionContext(
            message,
            _serviceProvider,
            _backend,
            maxRetriesLimit: 100
        );

        Assert.Equal(100, context.MaxRetries);
    }

    [Fact]
    public void MaxRetriesLimit_RetryBeyondTheClampIsRejected()
    {
        var message = CreateBrokerMessage("clamped-retry", ThrowingTask.TaskName).Message with
        {
            MaxRetries = int.MaxValue,
            Retries = 100,
        };

        var context = new TaskExecutionContext(
            message,
            _serviceProvider,
            _backend,
            maxRetriesLimit: 100
        );

        Assert.Throws<RejectException>(() => context.Retry());
    }

    private TaskExecutor CreateExecutor(
        Type taskType,
        string taskName,
        ResilienceOptions resilienceOptions
    )
    {
        var registry = new TaskRegistry();
        registry.Register(taskType, taskName);

        return new TaskExecutor(
            registry,
            _serviceProvider,
            _serializer,
            _backend,
            _revocationManager,
            _filterPipeline,
            Options.Create(
                new WorkerOptions { EnableRevocation = false, EnableRateLimiting = false }
            ),
            NullLogger<TaskExecutor>.Instance,
            resilienceOptions: Options.Create(resilienceOptions)
        );
    }

    private BrokerMessage CreateBrokerMessage(string taskId, string taskName, int retries = 0)
    {
        return new BrokerMessage
        {
            Message = new TaskMessage
            {
                Id = taskId,
                Task = taskName,
                Args = _serializer.Serialize(new TestInput { Value = 1 }),
                ContentType = _serializer.ContentType,
                Timestamp = DateTimeOffset.UtcNow,
                Queue = "celery",
                Retries = retries,
            },
            DeliveryTag = 1UL,
            Queue = "celery",
            ReceivedAt = DateTimeOffset.UtcNow,
        };
    }

    public async ValueTask DisposeAsync()
    {
        await _backend.DisposeAsync();
        await _serviceProvider.DisposeAsync();
        _revocationManager.Dispose();
    }

    private sealed class ThrowingTask : ITask<TestInput>
    {
        public static string TaskName => "auto.retry.throwing";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            throw new InvalidOperationException("Test exception");
        }
    }

    private sealed class RetryingTask : ITask<TestInput>
    {
        public static string TaskName => "auto.retry.retrying";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            context.Retry();
            return Task.CompletedTask;
        }
    }

    private sealed class RetryingWithCountdownTask : ITask<TestInput>
    {
        public static string TaskName => "auto.retry.retrying.countdown";

        public Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            context.Retry(countdown: TimeSpan.FromSeconds(30));
            return Task.CompletedTask;
        }
    }

    private sealed class TestInput
    {
        public int Value { get; set; }
    }
}
