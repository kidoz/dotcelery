using DotCelery.Core.Storage;

namespace DotCelery.Tests.Conformance.Storage;

/// <summary>
/// Conformance tests for <see cref="INotificationChannel"/>. They are skipped for providers
/// without notifications.
/// </summary>
public abstract class NotificationChannelConformanceTests : StorageConformanceTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private readonly string _channel = Unique("channel");

    [Fact]
    public async Task SubscribeAsync_ReceivesMessagesPublishedAfterSubscribing()
    {
        var notifications = RequireNotifications();
        using var cts = new CancellationTokenSource(Timeout);
        await using var subscription = notifications
            .SubscribeAsync(_channel, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        var received = await ReceiveWhilePublishingAsync(
            subscription,
            () => notifications.PublishAsync(_channel, "hello"),
            cts.Token
        );

        Assert.Equal("hello", received);
    }

    [Fact]
    public async Task SubscribeAsync_ReceivesOnlyItsChannel()
    {
        var notifications = RequireNotifications();
        var other = Unique("channel");
        using var cts = new CancellationTokenSource(Timeout);
        await using var subscription = notifications
            .SubscribeAsync(_channel, cts.Token)
            .GetAsyncEnumerator(cts.Token);

        var received = await ReceiveWhilePublishingAsync(
            subscription,
            async () =>
            {
                await notifications.PublishAsync(other, "other");
                await notifications.PublishAsync(_channel, "mine");
            },
            cts.Token
        );

        Assert.Equal("mine", received);
    }

    [Fact]
    public async Task SubscribeAsync_Cancelled_EndsTheSubscription()
    {
        var notifications = RequireNotifications();
        using var cts = new CancellationTokenSource();
        var subscription = notifications
            .SubscribeAsync(_channel, cts.Token)
            .GetAsyncEnumerator(cts.Token);
        var next = subscription.MoveNextAsync().AsTask();

        await cts.CancelAsync();

        try
        {
            Assert.False(await next.WaitAsync(Timeout));
        }
        catch (OperationCanceledException)
        {
            // Ending with a cancellation exception is also acceptable
        }

        await subscription.DisposeAsync();
    }

    // Providers may start listening asynchronously, so keep publishing until a message arrives
    private static async Task<string> ReceiveWhilePublishingAsync(
        IAsyncEnumerator<string> subscription,
        Func<ValueTask> publish,
        CancellationToken cancellationToken
    )
    {
        var next = subscription.MoveNextAsync().AsTask();

        while (!next.IsCompleted)
        {
            await publish();
            await Task.WhenAny(next, Task.Delay(50, cancellationToken));
        }

        Assert.True(await next);
        return subscription.Current;
    }

    private INotificationChannel RequireNotifications()
    {
        Assert.SkipWhen(
            Provider.Notifications is null,
            $"{Provider.Name} does not support notifications."
        );

        return Provider.Notifications!;
    }
}
