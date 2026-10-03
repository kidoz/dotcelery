# Roadmap

This document tracks shipped features, known gaps in them, and planned work.
Status and scope may change as the project evolves. Last reviewed: 2026-09-29.

Known gaps take priority over new features: several capabilities are exposed in the public API
but do not yet behave as documented.

## Completed Features

### Storage
- Every store is built once on the storage primitives (documents, leases, queues, counters, and notifications) and runs the same conformance tests on the in-memory, PostgreSQL, SQL Server, Redis, and MongoDB providers
- Versioned SQL migrations defined as schema operations, applied at startup under a database lock or exported as a script, with PostgreSQL and SQL Server dialects

### Brokers
- Redis broker (Redis Streams with consumer groups)
- RabbitMQ publisher confirms and mandatory routing

### Serialization
- Source-generated JSON contexts for core message types (`DotCeleryJsonContext`)
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
- Without a delay store, a message with a future ETA holds the worker's consume loop for up to 5 seconds before it is requeued
- Hard time limits are cooperative; a task that ignores its cancellation token keeps its worker slot

### Incomplete Documented Features
- Canvas: `Chain`, `Group`, and `Chord` define workflows, but nothing dispatches them or runs link and error callbacks
- Sagas: the orchestrator is not registered by the DI extensions
- Batches: `OnComplete` callbacks are not dispatched, and tasks are published before the batch record exists
- Metrics: `DotCeleryInstrumentation` defines instruments, but the client and worker never record them (tracing works)
- Dashboard: no built-in task query, queue stats, or metrics providers; workers do not register themselves; SignalR notifications are never raised; the middleware serves the UI page for API and hub routes unless endpoints are mapped first
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
- No tests exercise the worker consume/ack/retry/shutdown loop, the delayed-message and outbox dispatchers, or the tenant and overlap filters (the stores they use have conformance tests)
- The in-memory broker does not model redelivery or serialization round-trips; broker contract tests should run against real brokers
- No integration coverage for RabbitMQ connection loss
- Analyzer DCEL001 reports task names that are not string literals (for example, constants) as empty

## Planned Features

### Brokers
- Azure Service Bus broker
- Amazon SQS broker
- Broker contract extensions these require: lease renewal for long-running tasks, native delayed delivery, explicit dead-letter versus discard, and capability flags (priority, ordering, maximum message size)

### Serialization
- Pluggable serializers (MessagePack/Protobuf); brokers currently hard-code the JSON envelope
- Message compression
- Verified AOT and trimming compatibility (`IsAotCompatible`, no reflection-based task invocation)

### Task Registration and Dispatch
- Source generator for task registration and strongly-typed client helpers (replaces reflection in `AddTasksFromAssembly` and `CompiledTaskInvoker`)
- Interceptors to reduce dispatch overhead
- Extension members for fluent task signatures (after Canvas execution ships)

### Worker/Execution
- Exactly-once processing: the worker commits the inbox claim together with the result in one
  transaction (the stores can already write in the caller's transaction; the worker does not use
  one yet)
- Connection pooling controls for brokers: separate publish and consume connections with channel pooling for RabbitMQ
- Batch execution tasks (single-task processing of input batches)

### Security
- Message size limits enforced at publish time and before deserialization

### CLI
- `dotcelery` command-line tool (worker, beat, inspect, task management, queue ops)
