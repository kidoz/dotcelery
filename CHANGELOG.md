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
- The worker no longer drops a message when the result backend, revocation store, rate limiter, or retry publish fails; it returns the message to the broker
- Infrastructure failures after a task succeeds are no longer recorded as task failures
- Tasks interrupted by shutdown are returned to the broker instead of being recorded as failures, and are not requeued while they are still running
- A failed acknowledgement no longer stops a worker processing loop
- Message signing is thread-safe; the shared `HMACSHA256` instance could produce invalid signatures under concurrent use
- The Redis broker keeps consuming after transient errors and recreates a missing consumer group
- An expired message and a message the worker rejects are stored in the dead letter queue instead of being acknowledged silently, when a dead letter store is registered
- Signal handlers dispatched in process are resolved from a scope per dispatch, so scoped handlers are disposed with it instead of living in the root provider
- The tenant context of a task is now visible to the task itself: the executor enters the tenant scope around the task, because a value written to an `AsyncLocal` by `TenantContextFilter` (which runs inside the filter pipeline) never flowed back to its caller. The filter still rejects tasks whose tenant is not allowed and does not enter the scope for them
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
