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
- The delayed message, outbox, inbox, signal, revocation, partition lock, execution tracking, and rate limiting stores are built once in `DotCelery.Core.Storage.Stores` on the storage primitives, and run the same conformance tests on the in-memory and PostgreSQL providers
- `StorageStoreOptions` for those stores: claim timeout, outbox retries, retention, revocation polling, and a `Prefix` that keeps applications sharing storage apart
- `StoragePurgeService` deletes expired storage entries every `StorageStoreOptions.PurgeInterval`; `AddInMemoryStorage` and `AddPostgresStorage` register it

### Changed
- PostgreSQL stores no longer create their tables on first use; run migrations first (automatic with a generic host)
- PostgreSQL stores share one data source (connection pool) per connection string; `IPostgresDataSourceProvider` is a required constructor parameter
- Graceful shutdown stops taking messages first, returns prefetched messages to the broker, and closes the broker consumer only after every in-flight message is settled
- The worker stops with an error when the broker ends the message stream unexpectedly, instead of running without a consumer
- The Redis broker reads again immediately while messages are available; `RedisBrokerOptions.BlockTimeout` applies only after a read returns nothing
- The in-memory and PostgreSQL registrations of those eight stores use the stores built on the storage primitives. The `AddPostgres...` methods for them take `PostgresStorageOptions`, and their data moves from per-store tables to the storage tables
- In-memory revocation subscribers no longer receive revocations made before they subscribed, as with the other providers

### Removed
- `AutoCreateTables` from every PostgreSQL store's options
- The unused `DotCelery.Core.Migrations` framework and the Redis and MongoDB migration stores
- The `DotCelery.Build.SqlValidator` tool, which did not validate any SQL in this repository
- The per-store in-memory and PostgreSQL implementations of those eight stores and their options, and `InMemoryRateLimiter` from Core

### Fixed
- The worker no longer drops a message when the result backend, revocation store, rate limiter, or retry publish fails; it returns the message to the broker
- Infrastructure failures after a task succeeds are no longer recorded as task failures
- Tasks interrupted by shutdown are returned to the broker instead of being recorded as failures, and are not requeued while they are still running
- A failed acknowledgement no longer stops a worker processing loop
- Message signing is thread-safe; the shared `HMACSHA256` instance could produce invalid signatures under concurrent use
- The Redis broker keeps consuming after transient errors and recreates a missing consumer group
- The Redis broker returns buffered messages to their streams when consumption stops, adds a requeued copy before acknowledging the original, and no longer replaces its connection while reconnecting
- Delayed messages stay in the in-memory and PostgreSQL stores until they are dispatched; a dispatcher that fails or stops mid-batch leaves them to be claimed again
- Outbox messages and signals are claimed, so several dispatchers never handle the same one at once, and one whose dispatcher stops is delivered again after the claim timeout; outbox attempts count only failed publishes
- The PostgreSQL rate limiter no longer fails on every call, and the rate limit window is shared by every worker using the same storage
- Inbox and revocation entries expire, and task execution tracking and partition locks cannot be released by a task that no longer holds them

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
