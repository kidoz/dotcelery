using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json;
using DotCelery.Backend.InMemory.Storage;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Security;
using DotCelery.Core.Serialization;
using DotCelery.Core.Storage;
using DotCelery.Core.Storage.Stores;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Services;
using DotCelery.Worker;
using DotCelery.Worker.Execution;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace DotCelery.Tests.Unit.MultiTenancy;

public sealed class TenantIsolationTests
{
    private static readonly byte[] KeyA = Enumerable.Repeat((byte)11, 32).ToArray();
    private static readonly byte[] KeyB = Enumerable.Repeat((byte)22, 32).ToArray();

    [Theory]
    [InlineData("valid")]
    [InlineData("foreign-key")]
    [InlineData("wrong-queue")]
    [InlineData("missing-body")]
    [InlineData("tampered-envelope")]
    [InlineData("conflicting-header")]
    public async Task Worker_AuthenticatesTenantBeforeReadingOrWritingItsData(string scenario)
    {
        var security = new MessageSecurityOptions
        {
            EnableMessageSigning = true,
            TenantSigningKeys = new() { ["a"] = KeyA, ["b"] = KeyB },
        };
        using var validator = new MessageSecurityValidator(
            Options.Create(security),
            NullLogger<MessageSecurityValidator>.Instance
        );
        var tenants = new MultiTenancyOptions { Enabled = true, ValidTenants = ["a", "b"] };
        var router = new TenantRouter(Options.Create(tenants));
        router.AddTenantQueue("a", "queue-a");
        router.AddTenantQueue("b", "queue-b");
        var stores = Options.Create(new StorageStoreOptions { RequireTenantScope = true });
        var storage = new InMemoryStorageProvider();
        await using var backend = new ResultBackend(storage, stores);
        var counter = new Counter();
        await using var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(counter)
            .AddSingleton<IOptions<MessageSecurityOptions>>(Options.Create(security))
            .AddSingleton<IMessageSecurityValidator>(validator)
            .AddSingleton<ITenantRouter>(router)
            .AddTransient<EchoTask>()
            .BuildServiceProvider();
        var options = Options.Create(
            new WorkerOptions { EnableRevocation = false, EnableRateLimiting = false }
        );
        using var revocations = new RevocationManager(
            options,
            NullLogger<RevocationManager>.Instance
        );
        var registry = new TaskRegistry();
        registry.Register(typeof(EchoTask), EchoTask.TaskName);
        var executor = new TaskExecutor(
            registry,
            services,
            new JsonMessageSerializer(),
            backend,
            revocations,
            new TaskFilterPipeline(
                services,
                Options.Create(new TaskFilterOptions()),
                NullLogger<TaskFilterPipeline>.Instance
            ),
            options,
            NullLogger<TaskExecutor>.Instance,
            multiTenancyOptions: Options.Create(tenants)
        );
        var message = new TaskMessage
        {
            Id = "shared-id",
            Task = EchoTask.TaskName,
            Args = "{}"u8.ToArray(),
            ContentType = "application/json",
            Timestamp = DateTimeOffset.UtcNow,
            TenantId = "a",
            Queue = "queue-a",
            Headers =
                scenario == "conflicting-header"
                    ? new Dictionary<string, string> { ["X-Tenant-Id"] = "b" }
                    : null,
        };
        var body = JsonSerializer.SerializeToUtf8Bytes(
            message,
            DotCeleryJsonContext.Default.TaskMessage
        );
        var signature =
            scenario == "foreign-key"
                ? Convert.ToBase64String(HMACSHA256.HashData(KeyB, body))
                : validator.Sign(body);
        var delivery = new BrokerMessage
        {
            Message = scenario == "tampered-envelope" ? message with { Id = "spoofed" } : message,
            RawBody = scenario == "missing-body" ? null : body,
            Signature = signature,
            DeliveryTag = "delivery",
            Queue = scenario == "wrong-queue" ? "queue-b" : "queue-a",
            ReceivedAt = DateTimeOffset.UtcNow,
        };

        if (scenario == "valid")
        {
            Assert.Equal(
                TaskState.Success,
                (await executor.ExecuteAsync(delivery, "worker", CancellationToken.None)).State
            );
            Assert.Equal(1, counter.Calls);
            using (TenantContext.SetTenant("a"))
                Assert.NotNull(await backend.GetResultAsync("shared-id"));
        }
        else
        {
            await Assert.ThrowsAsync<RejectException>(() =>
                executor.ExecuteAsync(delivery, "worker", CancellationToken.None)
            );
            var broker = Substitute.For<IMessageBroker>();
            broker
                .ConsumeAsync(Arg.Any<IReadOnlyList<string>>(), Arg.Any<CancellationToken>())
                .Returns(call => DeliverAndWait(delivery, call.Arg<CancellationToken>()));
            var settled = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
#pragma warning disable CA2012 // NSubstitute consumes the configured ValueTask.
            broker
                .AckAsync(delivery, Arg.Any<CancellationToken>())
                .Returns(_ =>
                {
                    settled.TrySetResult();
                    return ValueTask.CompletedTask;
                });
#pragma warning restore CA2012
            using var worker = new CeleryWorkerService(
                broker,
                executor,
                options,
                NullLogger<CeleryWorkerService>.Instance
            );
            await worker.StartAsync(CancellationToken.None);
            await settled.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await worker.StopAsync(CancellationToken.None);
            var archived = Assert.Single(
                await storage
                    .Documents.QueryAsync("dotcelery.quarantine", DocumentFilter.All)
                    .ToListAsync()
            );
            Assert.NotEmpty(archived.Value.ToArray());
            Assert.Equal(0, counter.Calls);
            using (TenantContext.SetTenant("a"))
                Assert.Null(await backend.GetResultAsync("shared-id"));
        }
        using (TenantContext.SetTenant("b"))
            Assert.Null(await backend.GetResultAsync("shared-id"));
        Assert.Null(TenantContext.Current.TenantId);
    }

    private static async IAsyncEnumerable<BrokerMessage> DeliverAndWait(
        BrokerMessage delivery,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        yield return delivery;
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }

    [Fact]
    public void TenantKeys_CannotFallBackToTheSharedKeyOrReuseAnotherTenantsKey()
    {
        var options = new MessageSecurityOptions
        {
            EnableMessageSigning = true,
            SigningKey = KeyA,
            TenantSigningKeys = new() { ["a"] = KeyA },
        };
        using var validator = new MessageSecurityValidator(
            Options.Create(options),
            NullLogger<MessageSecurityValidator>.Instance
        );
        var message = TaskMessageFactory.ForTask(
            "test",
            "task",
            [],
            "application/json",
            DateTimeOffset.UtcNow,
            "queue",
            tenantId: "b"
        );
        var body = JsonSerializer.SerializeToUtf8Bytes(
            message,
            DotCeleryJsonContext.Default.TaskMessage
        );
        Assert.Throws<InvalidOperationException>(() => validator.Sign(body));
        Assert.False(
            validator.VerifySignature(body, Convert.ToBase64String(HMACSHA256.HashData(KeyA, body)))
        );
        options.TenantSigningKeys.Add("b", KeyA);
        Assert.Throws<InvalidOperationException>(options.Validate);
    }

    [Fact]
    public async Task DashboardRecords_QueriesAndIdsAreTenantScoped()
    {
        var records = new TaskRecordStore(
            Options.Create(new DashboardOptions()),
            new InMemoryStorageProvider(),
            Options.Create(new StorageStoreOptions { RequireTenantScope = true })
        );
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            records.GetAsync("same").AsTask()
        );
        using (TenantContext.SetTenant("a"))
            await records.RecordAsync(
                new()
                {
                    TaskId = "same",
                    TaskName = "private-a",
                    State = "Success",
                }
            );
        using (TenantContext.SetTenant("b"))
        {
            Assert.Null(await records.GetAsync("same"));
            Assert.Empty(await records.GetTaskNamesAsync());
            await records.RecordAsync(
                new()
                {
                    TaskId = "same",
                    TaskName = "private-b",
                    State = "Success",
                }
            );
            Assert.Equal(["private-b"], await records.GetTaskNamesAsync());
        }
        using (TenantContext.SetTenant("a"))
        {
            Assert.Equal("private-a", (await records.GetAsync("same"))!.TaskName);
            Assert.Equal(["private-a"], await records.GetTaskNamesAsync());
        }
    }

    private sealed class Counter
    {
        public int Calls { get; set; }
    }

    public sealed class Input;

    private sealed class EchoTask(Counter counter) : ITask<Input, Input>
    {
        public static string TaskName => "isolated.echo";

        public Task<Input> ExecuteAsync(
            Input input,
            ITaskContext context,
            CancellationToken cancellationToken = default
        )
        {
            counter.Calls++;
            Assert.Equal("a", context.TenantId);
            return Task.FromResult(input);
        }
    }
}
