using DotCelery.Client;
using DotCelery.Dashboard.Demo.Tasks;

namespace DotCelery.Dashboard.Demo.HostedServices;

/// <summary>
/// Continuously enqueues a small mix of demo tasks so the dashboard
/// has live activity to render across all panels and tabs.
/// </summary>
public sealed class TrafficGeneratorService(
    ICeleryClient client,
    ILogger<TrafficGeneratorService> logger
) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait a moment for the worker to come up.
        await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        logger.LogInformation("Starting traffic generator");

        var ticker = new PeriodicTimer(TimeSpan.FromSeconds(2));
        try
        {
            while (await ticker.WaitForNextTickAsync(stoppingToken))
            {
                await EnqueueBatchAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task EnqueueBatchAsync(CancellationToken cancellationToken)
    {
        // 2 emails, 3 calculations, 1 flaky task per tick → roughly mixed counters.
        for (var i = 0; i < 2; i++)
        {
            await client.SendAsync<EmailTask, EmailInput, EmailResult>(
                new EmailInput
                {
                    To = $"user{Random.Shared.Next(1000)}@example.com",
                    Subject = "Generated traffic",
                },
                cancellationToken: cancellationToken
            );
        }

        var ops = new[] { "+", "-", "*", "/" };
        for (var i = 0; i < 3; i++)
        {
            await client.SendAsync<CalculationTask, CalculationInput, CalculationResult>(
                new CalculationInput
                {
                    A = Random.Shared.Next(1, 100),
                    B = Random.Shared.Next(1, 20),
                    Operation = ops[Random.Shared.Next(ops.Length)],
                },
                cancellationToken: cancellationToken
            );
        }

        await client.SendAsync<FlakyTask, FlakyInput, FlakyResult>(
            new FlakyInput { FailureRatePercent = 30 },
            cancellationToken: cancellationToken
        );
    }
}
