using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Batches;
using DotCelery.Core.Canvas;
using DotCelery.Core.Extensions;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Sagas;
using DotCelery.Core.Security;
using DotCelery.Worker.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;

namespace DotCelery.Tests.Unit.Worker;

public sealed class WorkflowRecoveryTests
{
    [Fact]
    public async Task Recovery_ReachesPendingWorkBehindAFullPageOfPublishedSagas()
    {
        var time = new FakeTimeProvider();
        await using var broker = new InMemoryBroker();
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(time)
            .AddSingleton<IMessageBroker>(broker);
        services.AddDotCelery(builder =>
            builder.UseInMemoryBackend().AddInMemorySagaStore().AddSagaSupport()
        );
        await using var provider = services.BuildServiceProvider();
        var sagas = provider.GetRequiredService<ISagaStore>();
        for (var i = 0; i < 100; i++)
        {
            var saga = CreateSaga($"published-{i}", time.GetUtcNow().AddSeconds(i));
            await sagas.CreateAsync(
                saga with
                {
                    Steps =
                    [
                        saga.Steps[0] with
                        {
                            State = SagaStepState.Executing,
                            ExecutionDispatched = true,
                            ExecuteTaskId = $"already-published-{i}",
                        },
                    ],
                }
            );
        }
        await sagas.CreateAsync(CreateSaga("pending-after-page", time.GetUtcNow().AddSeconds(100)));
        var recovery = Assert.Single(
            provider.GetServices<IHostedService>(),
            service => service.GetType().Name == "WorkflowRecoveryService"
        );
        await recovery.StartAsync(CancellationToken.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            while (broker.GetQueueLength("celery") == 0)
            {
                time.Advance(TimeSpan.FromSeconds(1));
                await Task.Delay(10, timeout.Token);
            }
            await foreach (var delivery in broker.ConsumeAsync(["celery"], timeout.Token))
            {
                Assert.Equal("execute", delivery.Message.Task);
                Assert.Equal(
                    (await sagas.GetAsync("pending-after-page"))!.CurrentStep!.ExecuteTaskId,
                    delivery.Message.Id
                );
                await broker.AckAsync(delivery);
                break;
            }
            Assert.Equal(0, broker.GetQueueLength("celery"));
        }
        finally
        {
            await recovery.StopAsync(CancellationToken.None);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Restart_RecoversClaimedCallbackExecutionAndCompensation(bool isolated)
    {
        var time = new FakeTimeProvider();
        await using var broker = new InMemoryBroker();
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(time)
            .AddSingleton<IMessageBroker>(broker);
        services.AddDotCelery(builder =>
        {
            builder
                .UseInMemoryBackend()
                .AddInMemoryBatchStore()
                .AddInMemorySagaStore()
                .AddBatchSupport()
                .AddSagaSupport();
            if (isolated)
                builder.AddTenantRouting(options => options.Enabled = true);
        });
        services.Configure<MessageSecurityOptions>(options =>
        {
            options.EnableMessageSigning = isolated;
            options.TenantSigningKeys["a"] = Enumerable.Repeat((byte)1, 32).ToArray();
        });
        await using var provider = services.BuildServiceProvider();
        if (isolated)
            provider.GetRequiredService<ITenantRouter>().AddTenantQueue("a", "queue-a");
        var queue = isolated ? "queue-a" : "celery";
        var batches = provider.GetRequiredService<IBatchStore>();
        var sagas = provider.GetRequiredService<ISagaStore>();
        using (TenantContext.SetTenant(isolated ? "a" : null, isolated ? queue : null))
        {
            await batches.CreateAsync(
                new Batch
                {
                    Id = "batch",
                    State = BatchState.Pending,
                    CreatedAt = time.GetUtcNow(),
                    TaskIds = ["header"],
                    Callback = new BatchCallback
                    {
                        TaskName = "callback",
                        TaskId = "callback-id",
                        Queue = "celery",
                    },
                }
            );
            await batches.MarkTaskCompletedAsync("batch", "header");
            Assert.NotNull(await batches.TryClaimCallbackAsync("batch"));
            await sagas.CreateAsync(CreateSaga("execution", time.GetUtcNow()));
            Assert.NotNull(
                await sagas.TryClaimStepDispatchAsync("execution", "step", "execute-id")
            );
            await sagas.CreateAsync(CreateSaga("compensation", time.GetUtcNow()));
            await sagas.UpdateStepStateAsync("compensation", "step", SagaStepState.Completed);
            await sagas.UpdateStateAsync("compensation", SagaState.Compensating);
            Assert.NotNull(
                await sagas.TryClaimStepDispatchAsync(
                    "compensation",
                    "step",
                    "compensate-id",
                    compensation: true
                )
            );
        }
        time.Advance(TimeSpan.FromMinutes(10));
        var recovery = Assert.Single(
            provider.GetServices<IHostedService>(),
            service => service.GetType().Name == "WorkflowRecoveryService"
        );
        await recovery.StartAsync(CancellationToken.None);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            var messages = new List<TaskMessage>();
            await foreach (var delivery in broker.ConsumeAsync([queue], timeout.Token))
            {
                messages.Add(delivery.Message);
                await broker.AckAsync(delivery, timeout.Token);
                if (messages.Count == 3)
                    break;
            }
            Assert.Equal(
                ["callback-id", "compensate-id", "execute-id"],
                messages.Select(message => message.Id).Order()
            );
            Assert.All(
                messages,
                message =>
                {
                    Assert.Equal(queue, message.Queue);
                    Assert.Equal(isolated ? "a" : null, message.TenantId);
                }
            );
            using var scope = TenantContext.SetTenant(isolated ? "a" : null);
            while (
                !(await batches.GetAsync("batch"))!.CallbackPublished
                || !(await sagas.GetAsync("execution"))!.Steps[0].ExecutionDispatched
                || !(await sagas.GetAsync("compensation"))!.Steps[0].CompensationDispatched
            )
                await Task.Delay(10, timeout.Token);
            Assert.Equal(0, broker.GetQueueLength(queue));
        }
        finally
        {
            await recovery.StopAsync(CancellationToken.None);
        }
    }

    private static Saga CreateSaga(string id, DateTimeOffset now) =>
        new()
        {
            Id = id,
            Name = id,
            State = SagaState.Executing,
            CreatedAt = now,
            Steps =
            [
                new SagaStep
                {
                    Id = "step",
                    Name = "step",
                    Order = 0,
                    ExecuteTask = new Signature { TaskName = "execute" },
                    CompensateTask = new Signature { TaskName = "compensate" },
                },
            ],
        };
}
