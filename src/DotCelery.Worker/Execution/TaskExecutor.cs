using System.Diagnostics;
using System.Text.Json;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Exceptions;
using DotCelery.Core.Filters;
using DotCelery.Core.Instrumentation;
using DotCelery.Core.Models;
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Resilience;
using DotCelery.Core.Signals;
using DotCelery.Core.Storage.Stores;
using DotCelery.Worker.Filters;
using DotCelery.Worker.Registry;
using DotCelery.Worker.Resilience;
using DotCelery.Worker.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotCelery.Worker.Execution;

/// <summary>
/// Executes tasks using compiled delegates and DI.
/// </summary>
public sealed partial class TaskExecutor
{
    private readonly TaskRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly IMessageSerializer _serializer;
    private readonly IResultBackend _resultBackend;
    private readonly RevocationManager _revocationManager;
    private readonly TaskFilterPipeline _filterPipeline;
    private readonly ITaskSignalDispatcher _signalDispatcher;
    private readonly IRateLimiter? _rateLimiter;
    private readonly OutcomeRecorder? _outcomes;
    private readonly IInboxOutcomeStore? _inboxOutcomes;
    private readonly TenantDeliveryValidator _tenant;
    private readonly TaskInvocationPipeline _invocation;
    private readonly DeliveryQuarantine _quarantine;
    private readonly ResilienceOptions? _resilienceOptions;
    private readonly WorkerOptions _options;
    private readonly ILogger<TaskExecutor> _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="TaskExecutor"/> class.
    /// </summary>
    public TaskExecutor(
        TaskRegistry registry,
        IServiceProvider serviceProvider,
        IMessageSerializer serializer,
        IResultBackend resultBackend,
        RevocationManager revocationManager,
        TaskFilterPipeline filterPipeline,
        IOptions<WorkerOptions> options,
        ILogger<TaskExecutor> logger,
        ITaskSignalDispatcher? signalDispatcher = null,
        IRateLimiter? rateLimiter = null,
        TimeLimitEnforcer? timeLimitEnforcer = null,
        IInboxStore? inboxStore = null,
        IOptions<MultiTenancyOptions>? multiTenancyOptions = null,
        TimeProvider? timeProvider = null,
        IOptions<ResilienceOptions>? resilienceOptions = null
    )
    {
        _registry = registry;
        _serviceProvider = serviceProvider;
        _serializer = serializer;
        _resultBackend = resultBackend;
        _revocationManager = revocationManager;
        _filterPipeline = filterPipeline;
        _signalDispatcher = signalDispatcher ?? NullTaskSignalDispatcher.Instance;
        _rateLimiter = rateLimiter;
        _outcomes = OutcomeRecorder.Create(resultBackend, inboxStore);
        _inboxOutcomes = inboxStore as IInboxOutcomeStore;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;

        var tenantOptions = multiTenancyOptions?.Value;
        _tenant = new TenantDeliveryValidator(
            tenantOptions is { Enabled: true } ? tenantOptions : null,
            serviceProvider
        );
        _invocation = new TaskInvocationPipeline(filterPipeline, timeLimitEnforcer, _tenant);
        _quarantine = new DeliveryQuarantine(serviceProvider, resultBackend, _timeProvider);
        _resilienceOptions = resilienceOptions?.Value;
    }

    internal ValueTask QuarantineDeliveryAsync(
        BrokerMessage delivery,
        CancellationToken cancellationToken
    ) => _quarantine.WriteAsync(delivery, cancellationToken);

    internal bool RequiresTenantAuthentication => _tenant.RequiresAuthentication;

    internal IDisposable? EnterTenantScope(TaskMessage message) => _tenant.EnterScope(message);

    internal string? ValidateTenantDelivery(BrokerMessage delivery) => _tenant.Validate(delivery);

    private static Activity? StartProcessActivity(TaskMessage message)
    {
        var parentExtracted = TraceContextPropagation.TryExtract(
            message.Headers,
            out var parentContext
        );

        var activity = parentExtracted
            ? DotCeleryDiagnostics.ActivitySource.StartActivity(
                $"process {message.Task}",
                ActivityKind.Consumer,
                parentContext
            )
            : DotCeleryDiagnostics.ActivitySource.StartActivity(
                $"process {message.Task}",
                ActivityKind.Consumer
            );

        if (activity is not null)
        {
            activity.SetTag("messaging.system", "dotcelery");
            activity.SetTag("messaging.operation", "process");
            activity.SetTag("messaging.destination.name", message.Queue);
            activity.SetTag("celery.task.name", message.Task);
            activity.SetTag("celery.task.id", message.Id);
            if (message.CorrelationId is { } correlationId)
            {
                activity.SetTag("celery.correlation.id", correlationId);
            }
        }

        return activity;
    }
}
