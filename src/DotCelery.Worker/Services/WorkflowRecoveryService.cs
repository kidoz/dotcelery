using DotCelery.Core.Batches;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Sagas;
using DotCelery.Core.Security;
using DotCelery.Worker.Batches;
using DotCelery.Worker.Sagas;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Services;

// Durable workflow documents are the recovery log. Every publication uses its persisted ID.
internal sealed class WorkflowRecoveryService(
    IServiceProvider services,
    ILogger<WorkflowRecoveryService> logger,
    TimeProvider? timeProvider = null
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var clock = timeProvider ?? TimeProvider.System;
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var tenant in TenantWorkloads.GetTenants(services))
            {
                using var tenantScope = TenantContext.SetTenant(
                    tenant,
                    tenant is null
                        ? null
                        : services.GetRequiredService<ITenantRouter>().GetQueue(tenant)
                );
                await using var scope = services.CreateAsyncScope();
                try
                {
                    if (
                        scope.ServiceProvider.GetService<IBatchStore>() is { } batches
                        && scope.ServiceProvider.GetService<BatchCompletionHandler>() is { } handler
                    )
                    {
                        await foreach (
                            var batch in batches
                                .GetPendingCallbacksAsync(stoppingToken)
                                .ConfigureAwait(false)
                        )
                        {
                            try
                            {
                                await handler
                                    .DispatchCallbackAsync(batch, stoppingToken)
                                    .ConfigureAwait(false);
                            }
                            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                            {
                                logger.LogWarning(
                                    ex,
                                    "Callback recovery failed for batch {BatchId}",
                                    batch.Id
                                );
                            }
                        }
                    }
                    if (
                        scope.ServiceProvider.GetService<ISagaStore>() is { } sagas
                        && scope.ServiceProvider.GetService<ISagaOrchestrator>()
                            is SagaOrchestrator orchestrator
                    )
                    {
                        await foreach (
                            var saga in sagas
                                .GetByStateAsync(SagaState.Executing, int.MaxValue, stoppingToken)
                                .ConfigureAwait(false)
                        )
                        {
                            try
                            {
                                if (saga.CurrentStep?.State == SagaStepState.Completed)
                                    await orchestrator
                                        .ContinueStepAsync(
                                            saga.Id,
                                            saga.CurrentStep.Id,
                                            stoppingToken
                                        )
                                        .ConfigureAwait(false);
                                else
                                    await orchestrator
                                        .ExecuteNextStepAsync(saga, stoppingToken)
                                        .ConfigureAwait(false);
                            }
                            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                            {
                                logger.LogWarning(
                                    ex,
                                    "Step recovery failed for saga {SagaId}",
                                    saga.Id
                                );
                            }
                        }
                        await foreach (
                            var saga in sagas
                                .GetByStateAsync(
                                    SagaState.Compensating,
                                    int.MaxValue,
                                    stoppingToken
                                )
                                .ConfigureAwait(false)
                        )
                        {
                            try
                            {
                                await orchestrator
                                    .ExecuteNextCompensationAsync(saga, stoppingToken)
                                    .ConfigureAwait(false);
                            }
                            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                            {
                                logger.LogWarning(
                                    ex,
                                    "Compensation recovery failed for saga {SagaId}",
                                    saga.Id
                                );
                            }
                        }
                    }
                }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogWarning(ex, "Workflow recovery could not read its durable state");
                }
            }
            await Task.Delay(TimeSpan.FromSeconds(1), clock, stoppingToken).ConfigureAwait(false);
        }
    }
}

internal static class TenantWorkloads
{
    public static IEnumerable<string?> GetTenants(IServiceProvider services)
    {
        var options = services.GetService<IOptions<MultiTenancyOptions>>()?.Value;
        return options is { Enabled: true, RequireAuthenticatedTenants: true }
            ? services
                .GetRequiredService<IOptions<MessageSecurityOptions>>()
                .Value.TenantSigningKeys.Keys
            : new string?[] { null };
    }
}
