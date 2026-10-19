# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `WorkerOptions.InfrastructureFailureRequeueDelay` sets how long the worker waits before returning a message to the broker after an infrastructure failure
- `DotCelery.Storage.Sql`: tables, columns, indexes, and sequences defined in code; migrations as schema operations; and `SqlMigrator`, which applies versioned migrations when the host starts, under a database lock, and records them with checksums of their operations in a `dotcelery_migrations` table. SQL text lives only in each database's dialect
- `SqlMigrator.GenerateScript()` produces a re-runnable SQL script for databases where schema changes are applied by hand
- `AddPostgres...` registration methods for every PostgreSQL store; each also registers the store's migrations
- Storage primitives in `DotCelery.Core.Storage` (`IStorageProvider` with documents, leases, queues, counters, and notifications), an in-memory provider, and conformance tests that every provider runs; stores will be rebuilt on these primitives
- PostgreSQL provider of the storage primitives (`AddPostgresStorage`), implemented once in `DotCelery.Storage.Sql` with the PostgreSQL dialect and LISTEN/NOTIFY notifications; every one of its statements is checked against the migrated schema in tests
- Every store (results, batches, sagas, dead letters, queue metrics, historical metrics, delayed messages, outbox, inbox, signals, revocations, partition locks, execution tracking, and rate limiting) is built once in `DotCelery.Core.Storage.Stores` on the storage primitives, and runs the same conformance tests on the in-memory and PostgreSQL providers
- `StorageStoreOptions` for those stores: claim timeout, outbox retries, retention, result expiry, revocation and result polling, and a `Prefix` that keeps applications sharing storage apart
- `AddInMemoryDeadLetterStore` and `AddInMemoryHistoricalDataStore`
- Redis provider of the storage primitives (`AddRedisStorage`, `RedisStorageOptions`): each operation is a Lua script on keys that share one hash tag, expiry follows the application clock, notifications use pub/sub, and every store shares one connection per connection string
- `AddRedis...` registration methods for every store
- MongoDB provider of the storage primitives (`AddMongoStorage`, `MongoStorageOptions`): conditional updates and unique indexes for versions, leases, claims, and counters, pipeline updates for rate limit windows, and notifications through a capped collection read with tailable cursors, so no replica set is needed; every store shares one client per connection string
- `AddMongo...` registration methods for every store
- SQL Server backend (`DotCelery.Backend.SqlServer`): a dialect of `DotCelery.Storage.Sql` with the same storage tables and migrations as PostgreSQL, `UseSqlServer` and `AddSqlServer...` registration methods for every store, and migrations under `sp_getapplock`; every statement is checked against the migrated schema in tests. It has no notifications, so stores poll
- `SqlExecutor` retries an operation a dialect reports as safely retryable, such as a SQL Server deadlock victim, and reports a command cancelled by the caller as `OperationCanceledException` for every driver
- `StoragePurgeService` deletes expired storage entries every `StorageStoreOptions.PurgeInterval`; `AddInMemoryStorage` and `AddPostgresStorage` register it
- `ITransactionalStorage`: a provider can write store records in a transaction the caller owns. `OutboxStore.StoreAsync` and `InboxStore.MarkProcessedAsync` write in the caller's transaction when one is given, so the record commits or rolls back with the caller's other changes; a provider that cannot write in the given transaction refuses the record instead of storing it outside the transaction. The PostgreSQL and SQL Server providers implement it
- `IRevocationStore.GetRevocationsAsync` returns revoked tasks with their options and revocation time, so workers can restore them at startup
- `ITransactionalStorage.RunInTransactionAsync` runs store writes in a transaction the storage starts, commits, and rolls back, and `OutcomeRecorder` uses it to store a task result and mark the message processed in the inbox together. `TaskExecutor` records every outcome through it, so with inbox deduplication and a result backend and inbox store that share a transactional storage (PostgreSQL or SQL Server), a message is marked processed exactly when its result is stored: a worker that stops after the transaction commits does not run the task again, and one that stops before leaves nothing behind
- `RabbitMQBrokerOptions.ReconnectDelay` sets how long the broker waits before rebuilding its channel and consumers after the connection is lost
- `BeatOptions.PersistState` and `StatePath` remember when each schedule entry last ran, so a restarted scheduler resumes the schedule, and `RunMissedOnStartup` decides whether a run missed while the scheduler was stopped is run once at startup or skipped
- The Beat scheduler elects one scheduler per `BeatOptions.SchedulerName` among the ones sharing the configured storage (through its lease store), so several instances no longer run the same schedule; without storage every scheduler runs it, and a warning says so
- Batch completion callbacks: the worker that settles the last task of a batch publishes its `OnComplete` task once, however the batch finished, with the input given when the batch was created and the batch ID on the message. `Batch.Callback` carries the callback and `CallbackDispatchedAt` records the claim, with `IBatchStore.TryClaimCallbackAsync` and `ReleaseCallbackClaimAsync` claiming it, so a batch whose last tasks settle at the same time runs its callback once
- Canvas execution: `ICanvasClient` (`AddCanvasClient()`) sends chains, groups, and chords, and `TaskExecutor`/`CeleryWorkerService` run them. A chain travels with its first task's message (`TaskMessage.Chain`, `ChainStep`), and the worker that runs a step publishes the next one with the step's result as its input before acknowledging, so the chain continues without a scheduler or stored workflow and a failure stops it. A chord is a group tracked as a batch whose completion callback is its body
- `Signature.GetInput()` exposes the typed input of `Signature<TTask, ...>`, which the canvas client serializes into `Args` before sending
- `MessageSecurityOptions.MaxMessageAge` refuses signed messages older than the configured age (`MessageValidationError.MessageExpired`); it narrows the window a captured signed message can be replayed in, and messages it refuses are dead-lettered rather than run
- The client and the worker record the metrics that `DotCelery.Telemetry` defines: sends, receives, completions with duration, retries, queue time, and tasks in progress, through `DotCeleryMetrics` in `DotCelery.Core` (the telemetry package re-exports it, so `AddDotCeleryInstrumentation()` still registers the same `DotCelery` meter)
- `AddSagaSupport()` registers the saga orchestrator (`ISagaOrchestrator`) and the completion handlers that advance and compensate sagas, so sagas can be started through the public API
- The default `JsonMessageSerializer` reads `JsonMessageSerializerOptions` from dependency injection, so the serializer options, including the deserialization type allowlist, can be configured with `services.Configure<JsonMessageSerializerOptions>(...)`; its properties are settable for that
- Canvas callbacks run: a task that succeeds publishes its `Signature.Link` with the task's result as the callback's input, and a task that fails publishes its `Signature.LinkError` with a `TaskErrorInfo` payload (the failed task's ID, name, and error). The callbacks travel on the message like chain steps, each step's callbacks travel with it, and a message is settled only once its callback is published
- The worker's circuit breakers gate consumption: while a queue's circuit is open, or the global one after failures that affected every queue, the queue's messages are returned to the broker instead of being run. Infrastructure failures feed the queue breaker and the global one, and a successful task clears the queue's failure count; task failures are left to retries and never stop a queue
- The worker registers itself with an `IWorkerRegistry` when one is registered — the dashboard registers one — and heartbeats its live active and processed task counts every `WorkerOptions.WorkerHeartbeatInterval`, unregistering on stop. `WorkerActivity` counts the work the heartbeat reports
- The dashboard serves real data through built-in providers: queue statistics from the `IQueueMetrics` store, task lists, state counts, and task names from a task record store written from task signals (`TaskRecordStore`, on the storage primitives, expiring after `DashboardOptions.TaskRecordRetention`), and metrics from the historical data store or computed live from the task records. An application no longer implements `IQueueStatsProvider`, `ITaskQueryProvider`, or `IMetricsProvider` itself; the dashboard API and hub serve these without any custom providers
- `DashboardMetricsCollector` records per-queue success and failure deltas into `IHistoricalDataStore` every `DashboardOptions.MetricsSnapshotInterval` and applies its retention hourly, so a dashboard's metrics survive the task record retention
- The dashboard raises its SignalR notifications: task signals record the task and push `TaskStateChanged` and `OverviewChanged`, and `DashboardWorkerMonitor` announces worker status changes and marks workers that missed `DashboardOptions.WorkerOfflineTimeout` unresponsive and then removes them
- `TaskSummary` is part of the source-generated JSON context, so task records serialize without reflection
- State-changing dashboard API requests must carry the dashboard request header (`X-DotCelery-Dashboard`, which the bundled UI sends and script clients set) and, when they carry an `Origin`, come from the dashboard's own origin or one of `DashboardOptions.AllowedOrigins`; `DashboardOptions.VerifyRequestOrigin` turns the check off
- `DashboardOptions.MaxBulkOperationSize` caps how many tasks one bulk operation may affect: a request with more explicit task IDs is refused, and a filter resolves at most that many. Task paging is clamped into `1..DashboardOptions.PageSize` with a bounded offset
- `RabbitMQBrokerOptions.QueueArguments` sets the arguments the broker declares its queues with — the queue type (quorum), a broker-level dead-letter exchange and routing key, a message TTL, a maximum length, the overflow policy, or any other broker argument — defaulting to a priority queue (`x-max-priority: 10`); `RabbitMQBrokerOptions.DurableQueues` still sets durability
- `WorkerOptions.MaxEtaWait` bounds how long a worker waits on a message whose ETA is still ahead when no delay store is configured
- `IBrokerCapabilities`: a broker declares what it guarantees beyond the delivery contract — priority delivery, native delayed delivery, redelivery of a message whose consumer stopped, and the maximum message size it accepts — so a caller can check its requirements instead of discovering the difference in production. The in-memory, RabbitMQ, and Redis brokers declare theirs
- `DotCelery.Broker.Sqs`: an Amazon SQS broker (`UseSqs(...)`) that maps a queue to an SQS queue, publishes a scheduled message with a delivery delay (SQS holds at most 15 minutes; the remainder is waited out by the worker), keeps a message being processed invisible so a task that runs long is not delivered twice, and holds a returned message hidden until its ETA. A rejected message is deleted, since dead-lettering is the queue's redrive policy. It declares its capabilities (native delay, redelivery, a 256 KiB message limit) and runs the broker contract suite against LocalStack
- A broker contract test suite runs against every broker (the in-memory broker in the unit suite, RabbitMQ and Redis against containers): message fields round-trip, delivery is addressed, settlement settles, rejection discards or redelivers, an unsettled message comes back when its consumer stops, priority is honored where the broker claims it, and concurrent publishes all arrive once
- Canvases nest: a group's member may be a chain (sent with its first task and its remaining steps, so it runs in order) or another group (flattened into its members), and a chord's header may hold chains and nested groups too. A chain member completes with its last step, so `GroupResult.TaskIds` and a chord's batch hold that step, and a chord over chains runs its callback once every chain finished
- `WorkerRegistry`: the worker registry on the storage primitives, so the workers and the dashboard of a deployment share one wherever they share storage. It registers, heartbeats, unregisters, lists, gets, and cleans up workers the way the in-process registry does, and a worker's record expires after `StorageStoreOptions.WorkerRetention` without a heartbeat, so a worker that stops without unregistering stops being listed even when no dashboard cleans up. `AddRedisWorkerRegistry`, `AddPostgresWorkerRegistry`, `AddSqlServerWorkerRegistry`, `AddMongoWorkerRegistry`, and `AddInMemoryWorkerRegistry` register it, and it runs the same conformance tests as the in-process registry on every provider
- The dashboard's queue statistics count the consumers of each queue from the worker registry — the online workers whose registered queues contain it — so the queue panel's consumer count is no longer always zero; a queue no online worker lists keeps the count the queue metrics store reports
- `WorkerInfo` and `WorkerStatus` are part of the source-generated JSON context, so registry records serialize without reflection
- The brokers record what they publish in the queue metrics store, so a queue's waiting count follows the messages in it: `InMemoryBroker`, `RabbitMQBroker`, `RedisBroker`, and `SqsBroker` take an optional `IQueueMetrics`, as they take their other collaborators, and record the enqueue once the message is in the queue. The record never fails a publish — a metrics store that is down must not read as a send that failed — and `QueueMetricsExtensions.RecordEnqueueAsync` is what a custom broker records with

### Changed
- PostgreSQL stores no longer create their tables on first use; run migrations first (automatic with a generic host)
- PostgreSQL stores share one data source (connection pool) per connection string; `IPostgresDataSourceProvider` is a required constructor parameter
- Graceful shutdown stops taking messages first, returns prefetched messages to the broker, and closes the broker consumer only after every in-flight message is settled
- The worker stops with an error when the broker ends the message stream unexpectedly, instead of running without a consumer
- The Redis broker reads again immediately while messages are available; `RedisBrokerOptions.BlockTimeout` applies only after a read returns nothing
- The in-memory and PostgreSQL registrations of every store use the stores built on the storage primitives. `UsePostgres` and every `AddPostgres...` method take `PostgresStorageOptions`, and data moves from per-store tables to the storage tables
- The Redis result backend and stores use the stores built on the storage primitives. `UseRedis` and every `AddRedis...` method take `RedisStorageOptions`, and data moves to new keys under `RedisStorageOptions.KeyPrefix`
- The MongoDB result backend and stores use the stores built on the storage primitives. `UseMongo` and every `AddMongo...` method take `MongoStorageOptions`, and data moves to new collections under `MongoStorageOptions.CollectionPrefix`
- Waiting for a result returns only a final result (success, failure, revoked, or rejected); a stored retry no longer ends the wait
- Task state updates never replace a final state, and the metadata passed with a state update is not stored
- Snapshots of historical metrics with the same timestamp but different task names are both kept
- In-memory revocation subscribers no longer receive revocations made before they subscribed, as with the other providers
- `RevocationManager` restores the options of revocations that exist when a worker starts, instead of default options
- `Signature` no longer serializes its derived properties (`HasLink`, `IsScheduled`, `EffectiveEta`, and the like), so a signature carried on a message holds only its inputs
- The worker stores a successful task's result and its inbox record in one storage transaction when the stores share a transactional provider, and stores the result before the record otherwise, so a failure between them leaves the message to be processed again instead of marked without a result. A failure to write the record is no longer logged and ignored: the message is returned to the broker
- Security validation runs when the worker receives a message, before the task input is deserialized and before the task is recorded as started, instead of in a task filter that ran after both. A refused message is stored in the dead letter queue and acknowledged without being recorded as a task result
- The dashboard middleware serves the UI page only for its own paths: API and hub paths under its prefix pass through to their endpoints, and the prefix counts only as a whole path segment, so `/celeryjs` is not the dashboard. Running the middleware before routing no longer shadows the API, and the `UseWhen` workaround is not needed
- `DashboardDataService` prefers the dashboard's built-in providers and falls back to a broker or backend that implements the optional provider interfaces
- A message that arrives before its ETA waits in a worker task slot when no delay store is configured, instead of holding the intake loop for up to five seconds, so other queues keep flowing while it waits; when the wait covers the ETA the task runs without a round trip through the broker, and a message that expires while waiting is dead-lettered. Task receive and queue-time metrics are now recorded when a message starts processing, so a message waiting on its ETA is not counted again on every redelivery
- The broker contract accepts at-least-once duplicates when several messages are published at once (standard SQS may deliver a message twice) and carries a past ETA in the field-preservation case, since a broker with native delay holds a message with a future one
- The in-memory broker models what the other brokers do, so tests that use it exercise realistic behavior: a delivered message is what its serialized payload says, an unsettled message whose consumer stops is delivered again, and an acknowledged or rejected message is settled. It delivers in publish order and holds no message until its ETA, which its declared capabilities say
- `Signature` is a canvas primitive: it derives from `CanvasPrimitive`, its `Type` is `CanvasType.Signature` (not serialized, so a signature carried on a message still holds only its inputs), and it yields itself from `GetSignatures()`, so a signature, a chain, and a group can be mixed in one `Group`. The `Group(IEnumerable<Signature>)`, `Group(params ReadOnlySpan<Signature>)`, and `Group.And(Signature)` overloads are gone, subsumed by their `CanvasPrimitive` counterparts. Sending a group or a chord whose members are chains or nested groups no longer throws `NotSupportedException`; a chord that is a member of a group still does
- `AddDotCeleryDashboard` registers its in-process `InMemoryWorkerRegistry` only when the application has not registered a registry, so a shared one registered by a provider is used whichever order the two registrations happen in
- Recording an enqueue is the broker's job: an application no longer calls `IQueueMetrics.RecordEnqueuedAsync` for what the broker publishes, which would count a message twice. The method stays for a queue filled outside a broker, and its documentation says so
- `Group.Then` builds a chord, as `WithCallback` does, instead of a chain whose first step was an internal group placeholder that the client published as a `__group__` task no worker could run; the placeholder is gone
- Task records are indexed by task name as well as by state: every write stores the record and an entry in a name index that expires with it, so listing by name reads the records of that name instead of scanning the retained ones — a name whose records were older than the newest thousand updates is no longer missed, and the listing answers in one query per page. Records written before the upgrade appear under their name once they are updated again or expire
- The dashboard's historical metrics break down by task name: the task signal handler counts what each name settled and how long it took, and `DashboardMetricsCollector` records a per-task snapshot every `DashboardOptions.MetricsSnapshotInterval` next to the per-queue ones, so the metrics panel's per-task view keeps filling after the task records expire. The cumulative totals live in `TaskMetricsStore` on the storage primitives, with a marker per name so the collector lists them without scanning

### Removed
- `AutoCreateTables` from every PostgreSQL store's options
- The unused `DotCelery.Core.Migrations` framework and the Redis and MongoDB migration stores
- The `DotCelery.Build.SqlValidator` tool, which did not validate any SQL in this repository
- `IRevocationStore.GetRevokedTaskIdsAsync`, replaced by `GetRevocationsAsync`, which also returns each revocation's options and time
- The per-store in-memory and PostgreSQL implementations and their options, including `PostgresBackendOptions`, and `InMemoryRateLimiter` from Core
- The per-store Redis implementations and their options, including `RedisBackendOptions`, and `RedisBackendJsonContext`
- The per-store MongoDB implementations and their options, including `MongoBackendOptions`
- `SecurityValidationFilter`, whose validation `UseMessageSecurity()` now performs in the worker before the message reaches the executor

### Fixed
- The inbox keeps a processed message ID for at least `MessageSecurityOptions.MaxMessageAge` when one is configured, so a replay inside the signing window is refused by the inbox even when `StorageStoreOptions.InboxRetention` is shorter
- A historical metrics snapshot is keyed by its queue and task name, so the snapshots a collector records for several queues at one instant are all kept; before, they shared a key and only the last queue of each interval was counted
- The worker no longer drops a message when the result backend, revocation store, rate limiter, or retry publish fails; it returns the message to the broker
- Infrastructure failures after a task succeeds are no longer recorded as task failures
- Tasks interrupted by shutdown are returned to the broker instead of being recorded as failures, and are not requeued while they are still running
- A failed acknowledgement no longer stops a worker processing loop
- Message signing is thread-safe; the shared `HMACSHA256` instance could produce invalid signatures under concurrent use
- The Redis broker keeps consuming after transient errors and recreates a missing consumer group
- An expired message and a message the worker rejects are stored in the dead letter queue instead of being acknowledged silently, when a dead letter store is registered
- Signal handlers dispatched in process are resolved from a scope per dispatch, so scoped handlers are disposed with it instead of living in the root provider
- The tenant context of a task is now visible to the task itself: the executor enters the tenant scope around the task, because a value written to an `AsyncLocal` by `TenantContextFilter` (which runs inside the filter pipeline) never flowed back to its caller. The filter still rejects tasks whose tenant is not allowed and does not enter the scope for them
- The queue metrics waiting count no longer goes negative when tasks start without a recorded enqueue (nothing in the library records enqueues), so the dashboard shows zero instead of a negative queue depth
- Dashboard authorization runs in the authorization stage, before model binding, so a refused request is never bound or executed; it ran as an action filter, after the request body was bound
- The DCEL001 analyzer evaluates `TaskName` through the semantic model: constant names and constant expressions the compiler folds are accepted and compared for duplicate-name detection, instead of being reported as empty, and a name computed at run time is no longer reported
- The Redis broker renews the claims of the deliveries its live consumption is still processing, and only those: a message a stopped consumption left unsettled used to stay pinned by the broker's next consumption (which renewed every pending entry of its consumer), so no one reclaimed it until the process ended
- Starting compensation for a saga whose step failed no longer returns early: the store marks such a saga `Compensating` while its compensation is still to run, which the orchestrator mistook for compensation already underway, so compensated steps were never compensated
- The Redis broker renews the claim on a message it is still processing, every third of `RedisBrokerOptions.ClaimTimeout`, so a task that runs longer than the claim timeout is no longer reclaimed by another consumer and executed twice. A message whose consumer stopped is still reclaimed after the timeout
- The Redis broker deletes a stream entry when it is settled, so a stream stays proportional to the work in flight instead of growing for the lifetime of the queue
- Beat schedules without a previous run no longer evaluate against a baseline that moves with the clock: an interval entry first runs one interval after the scheduler starts, so a schedule shorter than a day no longer runs at startup and one longer than a day no longer never runs, and a new cron entry runs at its next occurrence instead of at startup. An entry whose runs were missed while the scheduler was stopped starts from the next interval or occurrence unless `RunMissedOnStartup` is set
- The RabbitMQ broker acknowledges and rejects a message only on the channel that delivered it, and refuses to settle a message whose channel was lost instead of using another channel, whose tags mean other messages. It owns reconnection rather than competing with the client's automatic recovery: a consume loop rebuilds its channel and consumers after the connection is lost, so the stream pauses across a broker restart instead of ending, and a closed connection is replaced only when the broker connects again
- The Redis broker returns buffered messages to their streams when consumption stops, adds a requeued copy before acknowledging the original, and no longer replaces its connection while reconnecting
- Delayed messages stay in the in-memory and PostgreSQL stores until they are dispatched; a dispatcher that fails or stops mid-batch leaves them to be claimed again
- Outbox messages and signals are claimed, so several dispatchers never handle the same one at once, and one whose dispatcher stops is delivered again after the claim timeout; outbox attempts count only failed publishes
- The PostgreSQL rate limiter no longer fails on every call, and the rate limit window is shared by every worker using the same storage
- Inbox and revocation entries expire, and task execution tracking and partition locks cannot be released by a task that no longer holds them
- Inbox deduplication marks a message as processed once its result is stored, so `UseInboxDeduplication()` no longer lets redelivered messages run again; the filter only skips duplicates and logs a warning when deduplication is enabled without an inbox store registered
- PostgreSQL results larger than 8000 bytes are stored; result notifications carry only the task ID
- The client's `Pending` state no longer makes waiting for a result return before the task finishes, and no longer overwrites a result that is already stored
- Batch completion is atomic, so tasks finishing together are all counted; the PostgreSQL store now finishes batches, and a cancelled batch stays cancelled
- A batch record is created before its tasks are published, so a task that a worker finishes immediately is counted and the batch can finish; a task that fails to publish, and the tasks after it, are recorded as failed so the batch does not wait for tasks that will never report
- PostgreSQL sagas are marked completed and compensated
- Requeueing a dead letter whose original message cannot be read keeps the dead letter instead of dropping it
- A task whose worker stopped counts as running in the queue metrics only until the execution timeout
- Redis: delayed messages stay stored until they are dispatched, outbox messages are claimed, dead letters expire one by one instead of together, saga updates no longer read mismatched property names, batch completion is atomic, and stores no longer open a new connection on every reconnect
- MongoDB: a delayed message is no longer dispatched twice, outbox messages are claimed, inbox and revocation entries expire, batches and sagas are finished, and the client's `Pending` state no longer overwrites a stored result or ends a wait early

## [0.1.0] - 2026-01-12

### Added

#### Core
- `ITask<TInput, TOutput>` and `ITask<TInput>` interfaces for task definition
- `ITaskContext` providing execution context (TaskId, RetryCount, TenantId, PartitionKey)
- `IMessageBroker` abstraction for message transport
- `IResultBackend` abstraction for result storage
- `ICeleryClient` for task sending and result retrieval
- Canvas workflow primitives: `Chain`, `Group`, `Chord`, `Signature`

#### Brokers
- In-memory broker for testing and development
- RabbitMQ broker with publisher confirms and mandatory routing
- Redis Streams broker with consumer groups

#### Backends
- In-memory backend for testing and development
- Redis backend with sorted sets for delayed messages
- PostgreSQL backend with migrations
- MongoDB backend with TTL indexes

#### Scheduling
- Beat scheduler for periodic task execution
- Self-contained cron parser (5/6/7-field, L/W/# support)
- ETA/Countdown support with delayed message store

#### Enterprise Features
- Task cancellation with pub/sub notifications
- Rate limiting with sliding window algorithm
- Task filters pipeline for cross-cutting concerns
- Task signals/events (PreRun, PostRun, Success, Failure)
- Pattern-based task routing (glob patterns)
- Soft/hard time limits with `SoftTimeLimitExceededException`
- Dead letter queue with configurable handlers
- Batches with completion tracking

#### Reliability
- Kill switch for auto-stop on failure threshold
- Circuit breaker for endpoint-level protection
- Graceful shutdown handler
- Outbox store abstraction and dispatcher
- Inbox store abstraction for message deduplication

#### Workflows
- Saga state machine for long-running processes
- Progress reporting via `IProgressReporter`
- Compensating actions for saga rollback

#### Advanced Patterns
- Partitioned messaging for sequential processing
- `PreventOverlapping` attribute for task deduplication
- Multi-tenancy with `ITenantRouter` and `ITenantContext`
- Queue metrics (WaitingCount, RunningCount, ProcessedCount)

#### Observability
- OpenTelemetry distributed tracing and metric instrument definitions
- Web dashboard with SignalR hub
- Worker registry store with heartbeat tracking

### Notes
- Requires .NET 10.0 and C# 14
- First public release

[Unreleased]: https://github.com/kidoz/dotcelery/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kidoz/dotcelery/releases/tag/v0.1.0
