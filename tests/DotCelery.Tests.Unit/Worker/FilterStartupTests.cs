using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory;
using DotCelery.Core.Abstractions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Filters;
using DotCelery.Worker.Extensions;
using DotCelery.Worker.Filters;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DotCelery.Tests.Unit.Worker;

public sealed class FilterStartupTests
{
    [Theory]
    [InlineData(typeof(PartitionedExecutionFilter))]
    [InlineData(typeof(PreventOverlappingFilter))]
    [InlineData(typeof(InboxDeduplicationFilter))]
    public async Task MissingRequiredFilterStore_FailsStartup(Type filterType)
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<IMessageBroker, InMemoryBroker>();
        services.AddDotCelery(builder => builder.UseInMemoryBackend().AddWorker());
        services.Configure<TaskFilterOptions>(options => options.GlobalFilterTypes.Add(filterType));
        await using var provider = services.BuildServiceProvider();
        var diagnostics = Assert.Single(
            provider.GetServices<IHostedService>(),
            service => service.GetType().Name == "WorkerStartupDiagnostics"
        );
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            diagnostics.StartAsync(CancellationToken.None)
        );
    }
}
