# Roadmap

This document tracks shipped features, known gaps in them, and planned work.
Status and scope may change as the project evolves. Last reviewed: 2026-10-08.

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
- The worker validates signatures, the task-name allowlist, schema version, payload size, and message age before the task input is deserialized and before the task is recorded as started; a refused message is dead-lettered
- Tenant validation against a configured tenant list, with the tenant context visible to the task
- Dashboard authorization filter applied to controllers, SignalR hub, and middleware
- Dashboard route prefixing via `DashboardRoutePrefixConvention`

### Task Registration and Dispatch
- Analyzer reports duplicate task names at compilation end
- `SendOptions.TenantId` and `SendOptions.PartitionKey` with tenant-aware queue routing

### Resilience
- Circuit breakers gate consumption per queue, with the global breaker covering failures that affected every queue; infrastructure failures feed both, and a task's own failure is left to its retries

## Known Gaps

### Delivery and Reliability
- RabbitMQ broker: queue arguments (priority, queue type, dead-letter exchange) are fixed and cannot be configured
- Without a delay store, a message with a future ETA holds the worker's consume loop for up to 5 seconds before it is requeued
- Hard time limits are cooperative; a task that ignores its cancellation token keeps its worker slot

### Incomplete Documented Features
- Canvas: groups of chains, groups, or chords are not supported
- Dashboard: no built-in task query, queue stats, or metrics providers; workers do not register themselves; SignalR notifications are never raised; the middleware serves the UI page for API and hub routes unless endpoints are mapped first

### Security Hardening
- A signed message can be replayed after `MessageSecurityOptions.MaxMessageAge` and the inbox retention have passed; an ID cannot be refused once its inbox record expires
- Dashboard: authorization before model binding, CSRF/origin checks on state-changing endpoints, and bounds on paging and bulk operations

### Test Coverage
- The in-memory broker does not model redelivery or serialization round-trips; broker contract tests should run against real brokers
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
- Connection pooling controls for brokers: separate publish and consume connections with channel pooling for RabbitMQ
- Batch execution tasks (single-task processing of input batches)

### Security
- Message size limits enforced at publish time and before deserialization

### CLI
- `dotcelery` command-line tool (worker, beat, inspect, task management, queue ops)
