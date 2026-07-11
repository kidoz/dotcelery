using DotCelery.Core.Abstractions;
using DotCelery.Core.HealthChecks;
using DotCelery.Core.Models;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using NSubstitute;

// CA2012 fires on NSubstitute's `mock.Method(...).Returns(...)` because the analyzer
// cannot see that the ValueTask returned by the proxy is consumed by the substitution
// machinery. The pattern is idiomatic and intentional in tests.
#pragma warning disable CA2012

namespace DotCelery.Tests.Unit.HealthChecks;

public sealed class DotCeleryHealthCheckTests
{
    [Fact]
    public async Task BrokerHealthCheck_ReportsHealthy_WhenBrokerSaysSo()
    {
        var broker = Substitute.For<IMessageBroker>();
        broker
            .IsHealthyAsync(Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromResult(true));
        var check = new DotCeleryBrokerHealthCheck(broker);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task BrokerHealthCheck_ReportsUnhealthy_WhenBrokerSaysFalse()
    {
        var broker = Substitute.For<IMessageBroker>();
        broker
            .IsHealthyAsync(Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromResult(false));
        var check = new DotCeleryBrokerHealthCheck(broker);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task BrokerHealthCheck_ReportsUnhealthy_OnException()
    {
        var broker = Substitute.For<IMessageBroker>();
        broker
            .IsHealthyAsync(Arg.Any<CancellationToken>())
            .Returns<ValueTask<bool>>(_ => throw new InvalidOperationException("boom"));
        var check = new DotCeleryBrokerHealthCheck(broker);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.NotNull(result.Exception);
    }

    [Fact]
    public async Task BackendHealthCheck_ReportsHealthy_WhenQueryRoundtrips()
    {
        var backend = Substitute.For<IResultBackend>();
        backend
            .GetResultAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromResult<TaskResult?>(null));
        var check = new DotCeleryBackendHealthCheck(backend);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task BackendHealthCheck_ReportsUnhealthy_OnException()
    {
        var backend = Substitute.For<IResultBackend>();
        backend
            .GetResultAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns<ValueTask<TaskResult?>>(_ => throw new InvalidOperationException("down"));
        var check = new DotCeleryBackendHealthCheck(backend);

        var result = await check.CheckHealthAsync(new HealthCheckContext());

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.NotNull(result.Exception);
    }

    [Fact]
    public async Task BrokerHealthCheck_PropagatesExternalCancellation()
    {
        var broker = Substitute.For<IMessageBroker>();
        broker
            .IsHealthyAsync(Arg.Any<CancellationToken>())
            .Returns<ValueTask<bool>>(call =>
            {
                var token = call.Arg<CancellationToken>();
                token.ThrowIfCancellationRequested();
                return ValueTask.FromResult(true);
            });
        var check = new DotCeleryBrokerHealthCheck(broker);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            check.CheckHealthAsync(new HealthCheckContext(), cts.Token)
        );
    }
}
