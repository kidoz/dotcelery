# Roadmap

This document tracks shipped features, known gaps in them, and planned work.
Status and scope may change as the project evolves. Last reviewed: 2026-09-24.

Known gaps take priority over new features: several capabilities are exposed in the public API
but do not yet behave as documented.

## Completed Features

### Brokers
- Redis broker (Redis Streams with consumer groups)
- RabbitMQ publisher confirms and mandatory routing

### Serialization
- Source-generated JSON contexts for core message types (`DotCeleryJsonContext`, `RedisBackendJsonContext`)
- Opt-in deserialization type allowlist (`JsonMessageSerializerOptions.EnforceDeserializationTypeAllowlist`)

### Security
- HMAC message signing across InMemory, RabbitMQ, and Redis brokers
- Worker-side validation filter for signatures, task-name allowlist, schema version, and payload size
- Tenant validation against a configured tenant list
- Dashboard authorization filter applied to controllers, SignalR hub, and middleware
- Dashboard route prefixing via `DashboardRoutePrefixConvention`

### Task Registration and Dispatch
- Analyzer reports duplicate task names at compilation end
- `SendOptions.TenantId` and `SendOptions.PartitionKey` with tenant-aware queue routing

## Known Gaps

### Delivery and Reliability
- Redis broker: pending-message reclaim takes over messages that live workers are still processing (no lease renewal), and acknowledged entries are never deleted from streams
- RabbitMQ broker: reconnect logic competes with the client's automatic recovery and can acknowledge on a different channel than the one that delivered the message; queue arguments (priority, queue type, dead-letter exchange) are fixed
- Delayed messages are removed from the store before they are published (Redis, PostgreSQL), and MongoDB can dispatch the same message twice
- Redis stores open a new connection on reconnect without disposing the old one
- Without a delay store, a message with a future ETA holds the worker's consume loop for up to 5 seconds before it is requeued
- Hard time limits are cooperative; a task that ignores its cancellation token keeps its worker slot

### Backend Correctness
- PostgreSQL rate limiter issues `SELECT COUNT(*) ... FOR UPDATE`, which PostgreSQL rejects
- PostgreSQL `LISTEN/NOTIFY` sends the full result as the payload; results over 8000 bytes roll back the write
- PostgreSQL and MongoDB: the client's `Pending` row makes `AsyncResult.GetAsync` return immediately, and the `Pending` write can overwrite a result that is already stored
- Redis dead-letter store sets a TTL on the hash that holds every entry
- Batch completion is a read-modify-write in the Redis and in-memory stores, and the PostgreSQL and MongoDB stores never advance batch state
- Outbox storage ignores the caller's transaction, and dispatch is not claim-safe across workers
- Inbox and revocation entries are never cleaned up, and Redis streams grow without bound

### Incomplete Documented Features
- Canvas: `Chain`, `Group`, and `Chord` define workflows, but nothing dispatches them or runs link and error callbacks
- Sagas: the orchestrator is not registered by the DI extensions, Redis saga scripts read property names that do not match the stored JSON, and the PostgreSQL and MongoDB stores never mark sagas completed
- Batches: `OnComplete` callbacks are not dispatched, and tasks are published before the batch record exists
- Metrics: `DotCeleryInstrumentation` defines instruments, but the client and worker never record them (tracing works)
- Dashboard: no built-in task query, queue stats, or metrics providers; workers do not register themselves; SignalR notifications are never raised; the middleware serves the UI page for API and hub routes unless endpoints are mapped first
- Inbox deduplication: `UseInboxDeduplication()` never marks messages as processed, so duplicates still run
- Beat: schedules without a previous run use a moving baseline (intervals over one day never fire, cron entries fire on startup), there is no leader election across instances, and `PersistState`/`StatePath` are unused
- Circuit breaker: `UseCircuitBreaker()` registers a factory that nothing uses
- Tenant context set by `TenantContextFilter` is not visible during task execution
- Scoped signal handlers are resolved from the root service provider

### Security Hardening
- Replay protection for signed messages (timestamp and expiry checks)
- Security validation before state writes and input deserialization, and dead-lettering of rejected messages (they are currently acknowledged)
- Serializer options, including the deserialization allowlist, configurable through dependency injection
- Dashboard: authorization before model binding, CSRF/origin checks on state-changing endpoints, and bounds on paging and bulk operations

### Test Coverage
- No tests exercise the worker consume/ack/retry/shutdown loop, the delayed-message and outbox dispatchers, or the inbox, tenant, and overlap filters
- The in-memory broker and stores do not model redelivery, serialization round-trips, or concurrent updates; contract tests should run against real brokers and backends
- No integration coverage for the Redis saga, inbox, outbox, and dead-letter stores, or for RabbitMQ connection loss
- No test checks every PostgreSQL statement against the migrated schema
- Analyzer DCEL001 reports task names that are not string literals (for example, constants) as empty

## Planned Features

### Storage
- Build every store once in Core on the storage primitives (`IStorageProvider`), so a backend implements only documents, leases, queues, counters, and notifications; the primitives, the in-memory provider, and the conformance tests exist
- `DotCelery.Storage.Sql`: schema defined in code, a generic migrator, per-database dialects, and validation of every statement against the migrated schema
- PostgreSQL, Redis, and MongoDB providers of the primitives, replacing their per-store implementations

### Brokers
- Azure Service Bus broker
- Amazon SQS broker
- Broker contract extensions these require: lease renewal for long-running tasks, native delayed delivery, explicit dead-letter versus discard, and capability flags (priority, ordering, maximum message size)

### Backends
- SQL Server backend as a dialect of `DotCelery.Storage.Sql`

### Serialization
- Pluggable serializers (MessagePack/Protobuf); brokers currently hard-code the JSON envelope, and Redis saga scripts decode JSON server-side
- Message compression
- Verified AOT and trimming compatibility (`IsAotCompatible`, no reflection-based task invocation)

### Task Registration and Dispatch
- Source generator for task registration and strongly-typed client helpers (replaces reflection in `AddTasksFromAssembly` and `CompiledTaskInvoker`)
- Interceptors to reduce dispatch overhead
- Extension members for fluent task signatures (after Canvas execution ships)

### Worker/Execution
- Exactly-once processing: atomic inbox claim committed together with result storage
- Connection pooling controls for brokers/backends: shared connections across Redis stores, separate publish and consume connections with channel pooling for RabbitMQ, and shared `MongoClient` instances across stores
- Batch execution tasks (single-task processing of input batches)

### Security
- Message size limits enforced at publish time and before deserialization

### CLI
- `dotcelery` command-line tool (worker, beat, inspect, task management, queue ops)
