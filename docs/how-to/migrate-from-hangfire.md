# Migrate a Hangfire operation

[Migration guides](migrate-to-dotcelery.md) / Hangfire

Use this guide for a Hangfire method invocation that can become an explicit
DotCelery task. Inventory the job's arguments, filters, queues, recurrence,
continuations, and retained records using the [common migration procedure](migrate-to-dotcelery.md).

Hangfire serializes a method and its arguments for a server to invoke later.
DotCelery instead routes a stable task name with a serialized input. See
[Hangfire invocation](https://docs.hangfire.io/en/latest/background-methods/calling-methods-in-background.html).

## 1. Wrap the business operation

Extract the method's reusable work into an application service. Use the
[`RebuildReportTask` example](migrate-to-dotcelery.md#2-introduce-an-explicit-task-boundary)
to wrap it, and register its dependencies in the DotCelery worker host.
Replace Hangfire-specific execution context, filters, and cancellation handling
with explicit inputs, worker configuration, and the task cancellation token.

## 2. Change the producer

| Existing operation      | Destination                                                                                |
| ----------------------- | ------------------------------------------------------------------------------------------ |
| `BackgroundJob.Enqueue` | `ICeleryClient.SendAsync` with the task/input types                                        |
| Delayed submission      | `SendOptions.Countdown` or `Eta`, with a verified delivery path                            |
| Named queue             | `SendOptions.Queue` plus matching worker queues                                            |
| Recurring definition    | A separately reviewed Beat entry, or the old scheduler submitting tasks through an adapter |
| Continuation or batch   | Explicit orchestration design; verify success/failure and callback input semantics         |
| Monitoring/history      | DotCelery dashboard and result/history stores for new tasks; retain old history separately |

The common guide's `ReportSubmission.SubmitAsync` is the immediate submission
replacement. For a delayed producer, this method uses the same task types:

```csharp
using DotCelery.Client;

internal static class ScheduledReportSubmission
{
    internal static async Task<string> SubmitLaterAsync(
        ICeleryClient client,
        string reportId,
        CancellationToken cancellationToken)
    {
        var pending = await client.SendAsync<RebuildReportTask, RebuildReport>(
            new RebuildReport(reportId),
            new SendOptions
            {
                Queue = "reports",
                Countdown = TimeSpan.FromMinutes(5)
            },
            cancellationToken);
        return pending.TaskId;
    }
}
```

## 3. Make retry and delay behavior explicit

Hangfire's automatic retry filter is part of its failure processing; carry its
effective policy into your migration inventory rather than relying on another
runtime's defaults. See [Hangfire exception handling](https://docs.hangfire.io/en/latest/background-processing/dealing-with-exceptions.html).

This example configures a local validation host with delayed storage, an enabled
dispatcher, and an explicit retry policy. Register your `IReportService`
implementation before building the host:

```csharp
using DotCelery.Backend.InMemory.Extensions;
using DotCelery.Broker.InMemory.Extensions;
using DotCelery.Client.Extensions;
using DotCelery.Core.Extensions;
using DotCelery.Core.Resilience;
using DotCelery.Worker.Extensions;

builder.Services.AddDotCelery(celery => celery
    .UseInMemoryBroker()
    .UseInMemoryBackend()
    .AddInMemoryDelayedMessageStore()
    .AddTask<RebuildReportTask>()
    .AddClient()
    .AddWorker(options =>
    {
        options.Queues = ["reports"];
        options.UseDelayQueue = true;
    })
    .AddDelayedMessageDispatcher()
    .UseResilience(options =>
    {
        options.DefaultRetryPolicy = RetryPolicy.Exponential(maxRetries: 3);
        options.AutoRetryOnFailure = true;
    }));
```

Choose durable providers for a rollout that must survive process loss. Verify
retryable exception selection, attempt counts, delays, and terminal failure using
your real operation. A matching retry count alone does not make the policies equal.

## 4. Transfer schedule ownership

Hangfire recurring definitions have their own identifiers and schedule processing.
Translate them separately from already-enqueued executions. See
[Hangfire recurrence](https://docs.hangfire.io/en/latest/background-methods/performing-recurrent-tasks.html).

For each migrated definition, decide its next occurrence, time zone, missed-run
behavior, and which scheduler owns it. Beat's current entry calculation uses
UTC; apply the [Quartz migration schedule checks](migrate-from-quartz.md#2-translate-the-schedule-deliberately)
to time-sensitive definitions from any framework.

Disable the migrated recurring definition before enabling its replacement while
keeping workers available for old executions chosen to drain. Keep old job IDs
mapped to new task IDs when resubmitting logical inputs. Copying Hangfire database
records into a DotCelery backend does not create executable DotCelery tasks.

## Verify before cutover

Exercise an immediate job, a delayed job, a retryable failure, a terminal failure,
and cancellation. Test continuation/batch completion separately if used. Reconcile
running jobs and scheduled future jobs before following the common
[cutover and rollback steps](migrate-to-dotcelery.md#6-cut-over-one-operation-family).
