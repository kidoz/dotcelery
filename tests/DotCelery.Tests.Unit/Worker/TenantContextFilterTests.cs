using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DotCelery.Tests.Unit.Worker;

/// <summary>
/// Runs a task through the filter pipeline, so the tenant context is proven to be visible
/// inside the task and restored after it, and an invalid tenant is proven to be refused.
/// </summary>
public sealed class TenantContextFilterTests : IAsyncDisposable
{
    private readonly List<ServiceProvider> _providers = [];
    private readonly List<ResultBackend> _backends = [];
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public async Task RunningATask_SeesTheTenantContextSetAroundIt()
    {
        var executor = CreateExecutor(
            new MultiTenancyOptions { Enabled = true, DefaultTenantId = "default" }
        );
        TenantAwareTask.Reset();

        var result = await executor.ExecuteAsync(
            CreateMessage("t1", tenantId: "acme"),
            "test-worker",
            CancellationToken.None
        );

        Assert.Equal(TaskState.Success, result.State);
        Assert.Equal("acme", TenantAwareTask.ObservedTenantId);
        Assert.Equal("acme", TenantAwareTask.ObservedAfterAwait);

        // The executor disposes its scope after the task, restoring the previous tenant
        Assert.Null(TenantContext.Current.TenantId);
    }

    [Fact]
    public async Task ATaskOfAnInvalidTenant_IsRejectedWithoutRunning()
    {
        var executor = CreateExecutor(
            new MultiTenancyOptions { Enabled = true, ValidTenants = ["acme"] }
        );
        TenantAwareTask.Reset();

        var result = await executor.ExecuteAsync(
            CreateMessage("t1", tenantId: "other"),
            "test-worker",
            CancellationToken.None
        );

        Assert.Equal(TaskState.Rejected, result.State);
        Assert.False(TenantAwareTask.Ran);
        Assert.Null(TenantContext.Current.TenantId);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var backend in _backends)
        {
            await backend.DisposeAsync();
        }

        foreach (var provider in _providers)
        {
            await provider.DisposeAsync();
        }
    }

    private TaskExecutor CreateExecutor(MultiTenancyOptions tenantOptions)
    {
        var backend = new ResultBackend(new InMemoryStorageProvider());
        _backends.Add(backend);

        var services = new ServiceCollection();
        services.AddTransient<TenantAwareTask>();
        services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddOptions();

        // The filter resolves its options from the scope, as UseTenantContext configures them
        services.AddSingleton<IOptions<MultiTenancyOptions>>(Options.Create(tenantOptions));
        var provider = services.BuildServiceProvider();
        _providers.Add(provider);

        var options = Options.Create(
            new WorkerOptions { EnableRevocation = false, EnableRateLimiting = false }
        );
        var registry = new TaskRegistry();
        registry.Register(typeof(TenantAwareTask), TenantAwareTask.TaskName);

        var filterOptions = new TaskFilterOptions();
        filterOptions.GlobalFilterTypes.Add(typeof(TenantContextFilter));

        return new TaskExecutor(
            registry,
            provider,
            _serializer,
            backend,
            new RevocationManager(options, NullLogger<RevocationManager>.Instance),
            new TaskFilterPipeline(
                provider,
                Options.Create(filterOptions),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance,
            multiTenancyOptions: Options.Create(tenantOptions)
        );
    }

    private BrokerMessage CreateMessage(string taskId, string? tenantId) =>
        new()
        {
            Message = new TaskMessage
            {
                Id = taskId,
                Task = TenantAwareTask.TaskName,
                Args = _serializer.Serialize(new TestInput()),
                ContentType = "application/json",
                Timestamp = DateTimeOffset.UtcNow,
                Queue = "celery",
                TenantId = tenantId,
            },
            DeliveryTag = Guid.NewGuid(),
            Queue = "celery",
            ReceivedAt = DateTimeOffset.UtcNow,
        };

    public sealed class TestInput { }

    public sealed class TenantAwareTask : ITask<TestInput>
    {
        public static string TaskName => "tests.tenant-aware";

        public static string? ObservedTenantId { get; private set; }

        public static string? ObservedAfterAwait { get; private set; }

        public static bool Ran { get; private set; }

        public static void Reset()
        {
            ObservedTenantId = null;
            ObservedAfterAwait = null;
            Ran = false;
        }

        public async Task ExecuteAsync(
            TestInput input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            Ran = true;
            ObservedTenantId = TenantContext.Current.TenantId;

            // The tenant flows across an await inside the task, not only to its first line
            await Task.Yield();
            ObservedAfterAwait = TenantContext.Current.TenantId;
        }
    }
}
