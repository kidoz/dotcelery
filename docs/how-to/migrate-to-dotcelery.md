# Migrate background work to DotCelery

[How-to guides](README.md) / Migrate to DotCelery

Use this procedure to move an existing background operation into DotCelery while
keeping ownership of pending work explicit. Start with one operation and its
producers, workers, schedules, and stored state. This is an application migration;
the repository does not supply an importer for another framework's storage or
message format.

Choose the source-specific guide:

- [Hangfire](migrate-from-hangfire.md).
- [Quartz.NET](migrate-from-quartz.md).
- [Coravel or a hosted service](migrate-from-coravel.md).
- [MassTransit or NServiceBus background work](migrate-from-message-bus.md).

Check the [compatibility reference](../reference/migration-compatibility.md) before
deciding which features to move. These instructions describe this .NET 10 source
checkout; record the versions of the source framework and DotCelery you deploy.

## 1. Inventory the existing operation

Record these facts before changing submission code:

| Item           | Record                                                                          |
| -------------- | ------------------------------------------------------------------------------- |
| Operation      | Business method, serialized inputs, dependencies, and owner                     |
| Identity       | Source job/message ID and a business operation key                              |
| Routing        | Queues, consumers, ordering requirements, concurrency, and tenant               |
| Scheduling     | Time zone, future executions, recurrence, calendars, and missed-run policy      |
| Failure        | Retry count, delays, terminal failures, dead letters, and cancellation behavior |
| Side effects   | Database transactions, external calls, idempotency, and outgoing messages       |
| Retained state | Pending/running work, future jobs, results, and active workflows                |
| Acceptance     | Expected business output, latency budget, and recovery checks                   |

Choose one disposition for every existing record: finish in the old runtime,
resubmit an explicitly translated payload, retain as history, or cancel through
the old runtime's supported controls. Include delayed jobs and recurring
definitions, not just the currently visible queue.

## 2. Introduce an explicit task boundary

Keep reusable business logic in an application service. Replace framework-specific
execution context with serialized inputs and dependencies resolved by the worker.
For example, this complete set of type declarations wraps an existing report
operation:

```csharp
using DotCelery.Core.Abstractions;

internal sealed record RebuildReport(string ReportId);

internal interface IReportService
{
    Task RebuildAsync(string reportId, CancellationToken cancellationToken);
}

internal sealed class RebuildReportTask(IReportService reports)
    : ITask<RebuildReport>
{
    public static string TaskName => "reports.rebuild.v1";

    public Task ExecuteAsync(
        RebuildReport input,
        ITaskContext context,
        CancellationToken cancellationToken = default)
    {
        return reports.RebuildAsync(input.ReportId, cancellationToken);
    }
}
```

Implement and register `IReportService` in the worker host. Its implementation
must enforce the operation's idempotency/transaction rules. Do not serialize an
HTTP context, scoped service, database context, delegate, or source framework's
execution context into the input.

Register `RebuildReportTask` with `AddTask<RebuildReportTask>()` or generated
registration. For a working host scaffold, follow
[Run your first task](../tutorials/first-task.md). A result-producing operation can
use `ITask<TInput, TOutput>` instead.

## 3. Configure the destination before switching producers

Choose a broker, result backend, and the optional stores the operation requires.
Use separate destination queue names and storage prefixes while both systems are
running. In-memory registrations are useful for local validation; they do not
carry work across independent processes or survive a process loss.

Match the producer's queue with the worker's configured queues. Explicitly map
retry policy, cancellation, tenant routing, and any ordering or concurrency
requirements. Register delayed storage and an enabled delayed dispatcher when
using that delivery path; `Countdown` or `Eta` alone is not proof that delayed
work will reach a worker. Consult the selected broker's capabilities as well.

## 4. Replace a producer call

With the task and application service registered in the destination worker, send
an explicit DTO from the producer. This method uses the types above:

```csharp
using DotCelery.Client;

internal static class ReportSubmission
{
    internal static async Task<string> SubmitAsync(
        ICeleryClient client,
        string reportId,
        CancellationToken cancellationToken)
    {
        var pending = await client.SendAsync<RebuildReportTask, RebuildReport>(
            new RebuildReport(reportId),
            new SendOptions { Queue = "reports" },
            cancellationToken);
        return pending.TaskId;
    }
}
```

Store the returned task ID with the source ID and business operation key in your
migration records. The sample `ReportId` identifies a report; if repeated rebuilds
are distinct operations, add a separate operation ID. A reused `TaskId` is not, by
itself, a guarantee that business effects cannot repeat.

## 5. Validate in an isolated workload

Use synthetic data or a business implementation whose side effects are explicitly
disabled for the test. A parallel run against real effects needs a deliberate
ownership and deduplication design.

Before cutover, verify:

- A known input produces the expected business output and can be traced to its task ID.
- Duplicate submission and redelivery do not duplicate an effect that must be unique.
- Retry, terminal failure, cancellation, and process restart meet the recorded policy.
- Upcoming schedule occurrences match across the required time zone and calendar boundaries.
- A worker outage retains work with the chosen durable providers, and recovery is observable.

Set workload-specific latency and recovery thresholds before evaluating them.
The [project checks](run-checks.md) exercise DotCelery itself; they do not replace
your application's migration acceptance tests.

## 6. Cut over one operation family

1. Pause creation of that operation in the old producer or scheduler.
2. Record a boundary and reconcile pending, in-flight, delayed, and failed work.
3. Drain work chosen to finish in the old runtime. Keep its workers available for it.
4. For resubmission, export logical inputs through the old application's supported
   interfaces, translate them, and publish through `ICeleryClient`. Do not copy
   storage rows or forward raw old envelopes to a DotCelery worker.
5. Switch the selected producer/schedule to DotCelery and enable its workers.
6. Reconcile the business operation ledger against both runtimes before retiring
   the old operation's workers or schedules.

Each operation must have one execution owner at the boundary. If export,
submission, and source acknowledgement are separate writes, use a durable
migration ledger/outbox and design for a repeated transfer. A successful publish
followed by a lost acknowledgement can otherwise produce a duplicate.

## 7. Keep rollback bounded

Keep the old binaries, configuration, and retained records until acceptance is
complete. To roll back, pause new DotCelery submissions, reconcile work it already
accepted or completed, and restore the old producer only for operations that
remain unowned. Do not replay the entire old backlog after new workers have
already produced effects.

For why these checks matter, read [Delivery and retries](../explanation/delivery-and-retries.md).
