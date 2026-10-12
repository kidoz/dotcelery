using DotCelery.Core.Dashboard;
using DotCelery.Dashboard.Extensions;
using DotCelery.Dashboard.Services;
using Microsoft.Extensions.DependencyInjection;

namespace DotCelery.Tests.Unit.Dashboard;

/// <summary>
/// The dashboard registers the in-process worker registry only when the application has not
/// registered one, so a registry shared with the other hosts — a provider registration such as
/// <c>AddRedisWorkerRegistry</c> — is what the dashboard uses.
/// </summary>
public sealed class DashboardWorkerRegistryRegistrationTests
{
    [Fact]
    public void WithoutARegistry_TheDashboardRegistersTheInMemoryOne()
    {
        var services = new ServiceCollection();

        services.AddDotCeleryDashboard();

        Assert.IsType<InMemoryWorkerRegistry>(
            services.BuildServiceProvider().GetRequiredService<IWorkerRegistry>()
        );
    }

    [Fact]
    public void ARegistryRegisteredFirst_IsKept()
    {
        var shared = new InMemoryWorkerRegistry();
        var services = new ServiceCollection();
        services.AddSingleton<IWorkerRegistry>(shared);

        services.AddDotCeleryDashboard();

        Assert.Same(shared, services.BuildServiceProvider().GetRequiredService<IWorkerRegistry>());
    }
}
