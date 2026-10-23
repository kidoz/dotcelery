# Migration compatibility

[Reference](README.md) / Migration compatibility

This table identifies concepts to evaluate when moving background operations to
DotCelery. A counterpart is a design starting point, not a storage, wire-format,
or behavioral compatibility guarantee. Upstream documentation was reviewed on
2026-10-23; check the documentation for the source version you actually run.

## Frameworks and operation boundaries

| Source              | Source concept                       | DotCelery counterpart                         | Boundary to verify                                                                     |
| ------------------- | ------------------------------------ | --------------------------------------------- | -------------------------------------------------------------------------------------- |
| Hangfire            | Serialized method invocation         | Named `ITask<TInput>` and input DTO           | Rewrite submission and invocation; retain or translate old jobs                        |
| Hangfire            | Recurring job                        | Beat entry                                    | Schedule identity, UTC occurrences, missed work, and ownership                         |
| Quartz.NET          | Job detail plus one or more triggers | Task plus one or more Beat entries            | Job data, trigger rules, calendars, overlap, and persistence                           |
| Coravel             | Queued invocable                     | Task with DI dependencies                     | Move captured state into a serialized input                                            |
| Coravel             | Fluent schedule                      | Beat interval/cron entry                      | First run, time zone, catch-up, and overlap semantics                                  |
| .NET hosted service | Loop or local queue                  | Worker tasks; optional Beat producer          | Separate process lifetime from individual work items                                   |
| MassTransit         | Command consumer                     | Task wrapping the eligible business operation | Receive topology, envelope, consumer context, outgoing messages, and transactions      |
| NServiceBus         | Command handler                      | Task wrapping the eligible business operation | Endpoint routing, recoverability, handler context, outgoing messages, and transactions |

Source concepts: [Hangfire invocation](https://docs.hangfire.io/en/latest/background-methods/calling-methods-in-background.html),
[Hangfire recurrence](https://docs.hangfire.io/en/latest/background-methods/performing-recurrent-tasks.html),
[Quartz jobs and triggers](https://www.quartz-scheduler.net/documentation/quartz-4.x/tutorial/jobs-and-triggers.html),
[Coravel queueing](https://docs.coravel.net/Queuing/),
[Coravel scheduling](https://docs.coravel.net/Scheduler/),
[.NET worker services](https://learn.microsoft.com/en-us/dotnet/core/extensions/workers),
[MassTransit consumers](https://masstransit.massient.com/concepts/consumers), and
[NServiceBus handlers](https://docs.particular.net/nservicebus/handlers/).

## Contracts that do not transfer automatically

| Concern                  | DotCelery behavior or migration constraint                                                                                                                         |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Stored jobs and messages | No cross-framework importer is supplied. Brokers carry DotCelery's own `TaskMessage` envelope.                                                                     |
| IDs and history          | Keep a source-to-destination mapping; old dashboards and history remain separate.                                                                                  |
| Priority                 | `SendOptions.Priority` is 0–9; broker support must be checked. It is not a universal ordering guarantee.                                                           |
| Retry                    | Specify the retry budget, eligible exceptions, and delays. Do not inherit another runtime's defaults by assumption.                                                |
| Delays                   | Verify the broker/store/dispatcher path through to execution and after restart.                                                                                    |
| Beat cron                | Beat parses standard five-field cron and evaluates it in UTC. The standalone cron library's other formats/time-zone overloads do not change Beat's entry behavior. |
| Beat startup             | `PersistState` defaults to false; `RunMissedOnStartup` defaults to false. Catch-up is not arbitrary trigger replay.                                                |
| Beat leadership          | Scheduler instances sharing storage elect a leader per `SchedulerName`; without storage each instance runs its schedule.                                           |
| Concurrency              | Worker concurrency, per-key exclusion, and a source framework's job/trigger locks have different scopes.                                                           |
| Cancellation             | Cooperative. An accepted request does not prove that execution or external effects stopped.                                                                        |
| Business effects         | Retries and transfers may repeat. Transactional outcome recording does not encompass arbitrary external side effects.                                              |
| Messaging patterns       | Do not equate a task send with publish/subscribe, a request/reply conversation, or an existing bus saga.                                                           |
| Active workflows         | State, correlation, timers, compensation, and outgoing messages need an explicit migration plan.                                                                   |

Source: [SendOptions](../../src/DotCelery.Client/SendOptions.cs),
[TaskMessage](../../src/DotCelery.Abstractions/Models/TaskMessage.cs),
[Beat entry calculation](../../src/DotCelery.Beat/ScheduleEntry.cs),
[Beat options](../../src/DotCelery.Beat/BeatOptions.cs), and
[delivery explanation](../explanation/delivery-and-retries.md).

Choose a [source-specific guide](../how-to/migrate-to-dotcelery.md), and check
[known gaps](../../ROADMAP.md#known-gaps) before committing to a feature mapping.
