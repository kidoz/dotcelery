# Roadmap

This document tracks shipped features, known gaps in them, and planned work.
Status and scope may change as the project evolves. Last reviewed: 2026-10-12.

Known gaps take priority over new features: several capabilities are exposed in the public API
but do not yet behave as documented.

## Completed Features

### Storage
- Every store is built once on the storage primitives (documents, leases, queues, counters, and notifications) and runs the same conformance tests on the in-memory, PostgreSQL, SQL Server, Redis, and MongoDB providers
- Versioned SQL migrations defined as schema operations, applied at startup under a database lock or exported as a script, with PostgreSQL and SQL Server dialects

### Brokers
- Redis broker (Redis Streams with consumer groups)
- RabbitMQ publisher confirms, mandatory routing, and configurable queue arguments (queue type, dead-lettering, TTL, length, overflow, priority depth)
- One broker contract, held to the same behavior by every broker through a shared conformance suite (in-memory in the unit suite, RabbitMQ, Redis, and Amazon SQS against containers), and `IBrokerCapabilities` for what a broker guarantees beyond it
- Amazon SQS broker: delivery delay for scheduled messages, visibility renewal while a task runs, discard on rejection with the queue's redrive policy for dead-lettering, and its declared capabilities

### Serialization
- Source-generated JSON contexts for core message types (`DotCeleryJsonContext`)
- Opt-in deserialization type allowlist (`JsonMessageSerializerOptions.EnforceDeserializationTypeAllowlist`)
- AOT and trimming compatibility: Core, Worker, Client, and the in-memory broker and backend are `IsAotCompatible` and free of AOT and trim warnings. A Native AOT application registers tasks through the source generator, registers its payload types with `JsonMessageSerializerOptions.Context`, and the storage stores serialize through a generated context; `samples/DotCelery.AotSmoke` is published natively by `just aot-smoke` and CI. The dashboard and the driver-based providers are not covered by this verification

### Security
- HMAC message signing across InMemory, RabbitMQ, and Redis brokers
- The inbox keeps a processed message ID at least as long as `MessageSecurityOptions.MaxMessageAge`, so a replay inside the signing window is refused by the inbox even when `InboxRetention` is shorter
- A message over a broker's maximum size is refused as it is published, and the worker refuses a payload over `MessageSecurityOptions.MaxPayloadSizeBytes` before the task input is deserialized
- The worker validates signatures, the task-name allowlist, schema version, payload size, and message age before the task input is deserialized and before the task is recorded as started; a refused message is dead-lettered
- Tenant validation against a configured tenant list, with the tenant context visible to the task
- Dashboard authorization runs before model binding and applies to controllers, the SignalR hub, and the middleware; state-changing API requests must carry the dashboard request header and an allowed origin, and paging and bulk operations are bounded
- Dashboard route prefixing via `DashboardRoutePrefixConvention`

### Task Registration and Dispatch
- Analyzer reports duplicate task names at compilation end
- A source generator registers the tasks an assembly defines under their static names through typed references, with no reflection: `DotCelery.Generated.DotCeleryTasks.RegisterTasks()` calls `AddTask<TTask>(name, invoker)` for each, `AddTask<TTask>()` records the name with the type, so the worker's registry is built without reading names through reflection, and the generated `ITaskInvoker` adapter is used to invoke the task instead of compiling an expression delegate
- The generator records each task's input and output types with the registration, so the worker's registry does not inspect a generated task's interfaces, and emits a typed signature helper (`Email(input)` returns `Signature<EmailTask, EmailInput, EmailResult>`), named after the task without its `Task` suffix, so canvas code does not spell out the type arguments
- `SendOptions.TenantId` and `SendOptions.PartitionKey` with tenant-aware queue routing

### Resilience
- Circuit breakers gate consumption per queue, with the global breaker covering failures that affected every queue; infrastructure failures feed both, and a task's own failure is left to its retries

### Dashboard
- Workers register themselves with the dashboard's registry and heartbeat their live active and processed task counts; the registry marks workers that miss their heartbeat budget unresponsive and then removes them. It is in-process by default, and `AddRedisWorkerRegistry`, `AddPostgresWorkerRegistry`, `AddSqlServerWorkerRegistry`, `AddMongoWorkerRegistry`, and `AddInMemoryWorkerRegistry` register one built on the storage primitives, which every host sharing that storage sees and which the same conformance tests hold to the same behavior on every provider
- Built-in data providers: queue statistics from the queue metrics store — whose waiting counts the brokers record as they publish, and whose consumers are counted from the worker registry — a task record store (written from task signals, expiring on a retention, and indexed by state and by task name) behind the task list, state counts, and task names, and metrics from the historical data store or computed live from the records, with a collector recording per-queue and per-task snapshots from the queue metrics and the per-task totals the signal handler keeps
- Task state changes and worker status changes are pushed over SignalR; the middleware passes API and hub paths through to their endpoints

## Known Gaps

### Delivery and Reliability
- Hard time limits are cooperative; a task that ignores its cancellation token keeps its worker slot

### Incomplete Documented Features
- Canvas: a chord cannot be a member of a group; following a group with a task is a chord, written with `WithCallback` or its workflow-named form `Then`
- Dashboard: the default worker registry is in-process, and so are the consumer counts taken from it, so a deployment whose workers run elsewhere registers a registry on shared storage

## Planned Features

### Brokers
- Azure Service Bus broker, with the contract extensions the other brokers have (lease renewal for long-running tasks, native delayed delivery, and explicit dead-letter versus discard); it has no portable local emulator, so a decision is needed on how its contract tests run

### Serialization
- Pluggable serializers (MessagePack/Protobuf); brokers currently hard-code the JSON envelope
- Message compression

### Task Registration and Dispatch
- Interceptors to reduce dispatch overhead
- Extension members for fluent task signatures (after Canvas execution ships)

### Worker/Execution
- Connection pooling controls for brokers: separate publish and consume connections with channel pooling for RabbitMQ
- Batch execution tasks (single-task processing of input batches)

### CLI
- `dotcelery` command-line tool (worker, beat, inspect, task management, queue ops)
