# Migrate a Quartz.NET job

[Migration guides](migrate-to-dotcelery.md) / Quartz.NET

Use this guide to move job execution to DotCelery and decide whether its trigger
can also move to Beat. Quartz models jobs and triggers separately; one job may
have multiple triggers. See [Quartz jobs and triggers](https://www.quartz-scheduler.net/documentation/quartz-4.x/tutorial/jobs-and-triggers.html).

## 1. Separate execution from trigger state

Wrap one execution of the job's business operation in a task, following the
[common task boundary](migrate-to-dotcelery.md#2-introduce-an-explicit-task-boundary).
Translate the effective job/trigger data into an input DTO. Keep source job and
trigger identities in migration records; do not pass `IJobExecutionContext` to a
DotCelery task.

Inventory calendars, multiple triggers, persistent job data updates, overlap
rules, recovery, and trigger listeners. Move scheduling only after those behaviors
have an explicit counterpart or a deliberate redesign. Keeping Quartz as the
schedule owner while it submits a DotCelery task is a valid intermediate step.

## 2. Translate the schedule deliberately

Quartz cron expressions include a seconds field and may include a year, and
triggers can carry time-zone and misfire behavior. See
[Quartz cron triggers](https://www.quartz-scheduler.net/documentation/quartz-3.x/tutorial/crontriggers.html).

Beat's current entries parse standard five-field cron and evaluate occurrences
in UTC. Its use of the standalone cron library does not expose that library's
other formats or time-zone overloads through a Beat entry.

For a reviewed requirement of **daily at 02:00 UTC**, register this entry in a
host with the report task, its service, and a DotCelery client already registered.
Add a reference to `DotCelery.Beat`:

```csharp
using DotCelery.Beat.Extensions;
using DotCelery.Beat;
using DotCelery.Core.Serialization;

var serializer = new JsonMessageSerializer();
builder.Services.AddBeatScheduler(
    schedule => schedule.AddCron<RebuildReportTask, RebuildReport>(
        "reports-daily",
        "0 2 * * *",
        new RebuildReport("daily"),
        serializer,
        new ScheduleOptions { Queue = "reports" }),
    options =>
    {
        options.SchedulerName = "reports";
        options.PersistState = true;
        options.StatePath = "reports.state.json";
        options.RunMissedOnStartup = false;
    });
```

The entry targets the `reports` queue, which its worker must consume. The state
path needs persistent storage and suitable access for the scheduler deployment.

Do not mechanically remove a cron field and assume equivalence. Compare upcoming
occurrences over representative dates, including daylight-saving transitions,
month ends, and excluded dates. For example, this checks a destination UTC
occurrence independently of the running scheduler:

```csharp
using DotCelery.Cron;

var expression = CronExpression.Parse("0 2 * * *");
var next = expression.GetNextOccurrence(
    new DateTimeOffset(2026, 10, 23, 0, 0, 0, TimeSpan.Zero));
Console.WriteLine(next?.ToString("O"));
```

Expected output:

```text
2026-10-23T02:00:00.0000000+00:00
```

## 3. Reconcile missed work and concurrency

`RunMissedOnStartup` decides whether Beat runs a missed entry once at startup.
It does not reproduce every Quartz misfire instruction, calendar, or per-trigger
recovery rule. Retain the source scheduler for rules whose semantics you have not
implemented and verified.

Worker concurrency is a host capacity setting. It does not automatically preserve
a Quartz job key's exclusion scope across workers. Identify the business resource
that must be exclusive and verify that rule under the destination's deployment
topology. For multiple Beat instances, shared storage and a consistent
`SchedulerName` are needed for its leader election; this is separate from task
execution overlap.

## 4. Cut over triggers as well as jobs

Stop future firings of the migrated source triggers, reconcile jobs already fired,
and enable the destination entry at the agreed boundary. Keep old scheduler
tables intact for history and rollback; they are not DotCelery schedule storage.

Verify startup, restart, a missed firing, the next expected execution, and the
required concurrency rule before retiring the source trigger. Follow the common
[cutover procedure](migrate-to-dotcelery.md#6-cut-over-one-operation-family).

Source: [Beat entries](../../src/DotCelery.Beat/ScheduleEntry.cs),
[Beat options](../../src/DotCelery.Beat/BeatOptions.cs), and
[cron formats](../../src/DotCelery.Cron/CronFormat.cs).
