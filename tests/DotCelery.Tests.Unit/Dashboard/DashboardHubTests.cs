using DotCelery.Core.Dashboard;
using DotCelery.Dashboard;
using DotCelery.Dashboard.Hubs;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace DotCelery.Tests.Unit.Dashboard;

public sealed class DashboardHubTests
{
    [Fact]
    public async Task WatchTask_UsesAPerConnectionLimitAndReleasesASlotWhenUnwatched()
    {
        var context = Substitute.For<HubCallerContext>();
        context.ConnectionId.Returns("connection");
        context.Items.Returns(new Dictionary<object, object?>());
        var groups = Substitute.For<IGroupManager>();
        using var hub = new DashboardHub(
            Substitute.For<IDashboardDataProvider>(),
            Options.Create(new DashboardOptions())
        )
        {
            Context = context,
            Groups = groups,
        };

        await Assert.ThrowsAsync<HubException>(() => hub.WatchTask("  "));
        await Assert.ThrowsAsync<HubException>(() => hub.WatchTask(new string('a', 513)));
        for (var i = 0; i < 100; i++)
            await hub.WatchTask($"task-{i}");

        await hub.WatchTask("task-0");
        await groups
            .Received(1)
            .AddToGroupAsync("connection", "task:task-0", Arg.Any<CancellationToken>());
        await Assert.ThrowsAsync<HubException>(() => hub.WatchTask("next"));
        await hub.UnwatchTask("task-0");
        await hub.WatchTask("next");
        await groups
            .Received(1)
            .AddToGroupAsync("connection", "task:next", Arg.Any<CancellationToken>());
    }
}
