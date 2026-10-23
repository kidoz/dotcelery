# Migrate Coravel or hosted-service work

[Migration guides](migrate-to-dotcelery.md) / Coravel and hosted services

Use this guide for a queued invocable, a scheduled operation, or a work item
currently processed by a custom hosted service.

Coravel's standard queue runs in memory and supports invocables and delegates.
Its scheduler has fluent timing and overlap controls. See
[Coravel queueing](https://docs.coravel.net/Queuing/) and
[scheduling](https://docs.coravel.net/Scheduler/). A .NET `BackgroundService` is a
hosted-service implementation; persistence and work ownership depend on the code
you build around it. See [Microsoft's worker-service documentation](https://learn.microsoft.com/en-us/dotnet/core/extensions/workers).

## 1. Define one bounded work item

Move one invocable execution or one iteration's business operation into an
`ITask<TInput>`, using the [shared task wrapper](migrate-to-dotcelery.md#2-introduce-an-explicit-task-boundary).
Represent captured values in a serializable DTO and inject application services
into the task. Replace invocable-specific context and shutdown tokens with the
task's execution inputs and cancellation token.

For a custom channel/queue worker, translate the queued data item. For an
indefinite polling loop, choose a bounded unit of work before creating a task;
keep the responsibility for discovering/enqueuing new work in a producer.

## 2. Change submission and scheduling independently

Replace the selected `QueueInvocable` or delegate submission with
[`ICeleryClient.SendAsync`](migrate-to-dotcelery.md#4-replace-a-producer-call).
Register the task and matching queue in the destination worker.

For recurring work, choose a Beat interval or a reviewed UTC cron entry. Check
first-run timing, missed work, time zones, and custom constraints against the old
operation. Beat intervals first run one interval after the scheduler anchors
them; startup execution needs a deliberate decision. See the
[schedule migration guide](migrate-from-quartz.md#2-translate-the-schedule-deliberately).

An overlap-prevention rule needs its original scope recorded: one process, one
job name, one input key, or all workers. A single destination worker with
concurrency one is not a distributed exclusion rule.

## 3. Choose what must survive restart

For a one-process comparison, the in-memory setup from
[your first task](../tutorials/first-task.md) is sufficient. For independent hosts
or recovery after process loss, choose shared durable providers and the feature
stores the operation needs.

If the old queue is transient, stop its producers and drain it while the process
remains alive. A restart before draining may eliminate the only copy of queued
inputs. If your hosted service uses a database cursor/checkpoint, reconcile that
checkpoint with the new task/business IDs instead of assuming its state is transient.

## 4. Remove the old execution owner after verification

Test submission, restart, shutdown cancellation, failure, and any overlap rule.
Then disable the migrated invocable schedule or hosted-service registration and
switch the producer according to the [common cutover procedure](migrate-to-dotcelery.md#6-cut-over-one-operation-family).

Keep Coravel features or hosted services that remain responsible for other work.
Moving one operation does not require replacing every hosted component in the application.
