# Roadmap

This document tracks implemented features, known gaps, and planned work in the checked-out
source. See [CHANGELOG.md](CHANGELOG.md) for release history.
Status and scope may change as the project evolves. Last reviewed: 2026-10-24.

Known gaps take priority over new features. Planned items have no committed release date.

## Completed Features

### Package Architecture
- `DotCelery.Abstractions` holds interfaces, models, options, and JSON contracts while retaining their `DotCelery.Core` namespaces; `DotCelery.Core` supplies implementations and references the abstractions package
- Registration-parity tests check that every provider exposes the same 14 store registrations and that all 14 interfaces resolve with the in-memory provider

### Storage
- Every store is built once on the storage primitives (documents, leases, queues, counters, and notifications) and runs the same conformance tests on the in-memory, PostgreSQL, SQL Server, Redis, and MongoDB providers
- Versioned SQL migrations defined as schema operations, applied at startup under a database lock or exported as a script, with PostgreSQL and SQL Server dialects
- PostgreSQL and SQL Server support outbox and inbox writes in a caller-owned transaction; successful task results and inbox records commit together when both stores share the same transactional storage

### Brokers
- Redis broker (Redis Streams with consumer groups)
- RabbitMQ publisher confirms, mandatory routing, and configurable queue arguments (queue type, dead-lettering, TTL, length, overflow, priority depth)
- One broker contract, held to the same behavior by every broker through a shared conformance suite (in-memory in the unit suite, RabbitMQ, Redis, and Amazon SQS against containers), and `IBrokerCapabilities` for what a broker guarantees beyond it
- Amazon SQS broker: delivery delay for scheduled messages, visibility renewal while a task runs, discard on rejection with the queue's redrive policy for dead-lettering, and its declared capabilities
- Explicit capabilities for holding requeued messages until their ETA and rejecting stale settlements; RabbitMQ settles on the delivery's original channel, and in-memory, Redis, and SQS track delivery ownership

### Serialization
- Source-generated JSON contexts for core message types (`DotCeleryJsonContext`)
- Opt-in deserialization type allowlist (`JsonMessageSerializerOptions.EnforceDeserializationTypeAllowlist`)
- AOT and trimming compatibility: Abstractions, Core, Worker, Client, and the in-memory broker and backend are `IsAotCompatible` and free of AOT and trim warnings. A Native AOT application registers tasks through the source generator, registers its payload types with `JsonMessageSerializerOptions.Context`, and the storage stores serialize through a generated context; `samples/DotCelery.AotSmoke` is published natively by `just aot-smoke` and CI. The dashboard and the driver-based providers are not covered by this verification

### Security
- HMAC message signing across in-memory, RabbitMQ, Redis, and Amazon SQS brokers
- The inbox keeps a processed message ID at least as long as `MessageSecurityOptions.MaxMessageAge`, so a replay inside the signing window is refused by the inbox even when `InboxRetention` is shorter
- A message over a broker's configured maximum size is refused as it is published; RabbitMQ, Redis, and SQS also enforce receive size limits before deserializing the envelope, and the worker checks payload size before deserializing task input
- Schema-version and payload-size validation apply to every worker message. Signing, the task-name allowlist, message-age checks, and tenant rejection are configurable; enabled checks run before task input deserialization and task-start recording, and refused messages enter the dead-letter path
- Startup warnings identify disabled signing, task allowlisting, and tenant validation; tenant validation without a configured tenant list is refused
- Enabled tenant isolation authenticates independent message keys and authorized delivery queues, scopes built-in stores and dashboard requests, and carries tenant context through continuations; trusted metadata routing remains an explicit opt-out
- Dashboard authorization runs before model binding and applies to controllers, the SignalR hub, and the middleware; state-changing API requests must carry the dashboard request header and an allowed origin, and paging and bulk operations are bounded
- Dashboard route prefixing via `DashboardRoutePrefixConvention`

### Task Registration and Dispatch
- Analyzer reports duplicate task names at compilation end
- A source generator registers the tasks an assembly defines under their static names through typed references, with no reflection: `DotCelery.Generated.DotCeleryTasks.RegisterTasks()` calls `AddTask<TTask>(name, invoker)` for each, `AddTask<TTask>()` records the name with the type, so the worker's registry is built without reading names through reflection, and the generated `ITaskInvoker` adapter is used to invoke the task instead of compiling an expression delegate
- The generator records each task's input and output types with the registration, so the worker's registry does not inspect a generated task's interfaces, and emits a typed signature helper (`Email(input)` returns `Signature<EmailTask, EmailInput, EmailResult>`), named after the task without its `Task` suffix, so canvas code does not spell out the type arguments
- `SendOptions.TenantId` and `SendOptions.PartitionKey` with tenant-aware queue routing
- `TaskMessageFactory` constructs initial messages and follow-ups consistently, preserving root, parent, tenant, and partition identifiers; `AddTenantRouting()` registers tenant routing independently of a provider package

### Workflows and Scheduling
- Canvas chains, groups, chords, nested canvases, and success/error links execute through `ICanvasClient` and workers; chain steps receive the previous result, and batch tracking drives group/chord completion callbacks
- `AddSagaSupport()` registers orchestration and completion handlers for advancing and compensating sagas
- Batch callbacks and saga execution/compensation use recoverable publication claims and stable task IDs; background scans resume interrupted publications
- Beat stores shared cursors and pending occurrences when storage is registered and renews leadership through publication; without storage, `PersistState` uses a local file
- `samples/DotCelery.CanvasExample` demonstrates chains, groups, chords, and nested canvases end to end

### Resilience
- Circuit breakers gate consumption per queue, with the global breaker covering failures that affected every queue; infrastructure failures feed both, and a task's own failure is left to its retries
- Per-task and worker-default retry policies provide constant or exponential backoff, jitter, and exception selection; requested retries without a countdown use the policy, and `AutoRetryOnFailure` enables policy-based automatic retries
- The worker clamps publisher-supplied retry budgets to `ResilienceOptions.MaxRetriesLimit` and delays infrastructure-failure requeues through `WorkerOptions.InfrastructureFailureRequeueDelay`
- Heartbeats and revocation subscriptions recover after backend outages; pending revocations expire with store retention
- Soft timeouts cancel and await the running execution before allowing a retry, observing late failures and preventing overlapping attempts
- Poison signals have bounded attempts across restarts and are archived before removal, with configurable failed-signal retention

### Dashboard
- React, TypeScript, Tailwind CSS, and Vite frontend with embedded assets, configurable prefixes, URL-backed filters and task links, and capability-aware retry, cancellation, and bulk operations; building from source requires Node.js 24.3 or later
- Explicit REST API versioning at `<prefix>/api/v1/`; unversioned REST routes are refused
- Active-view polling, cached filter lists, independent response rendering, and polling suspension while inspecting details or confirming an action; stale responses and action completions preserve newer navigation and dialogs
- Workers register themselves with the dashboard's registry and heartbeat their live active and processed task counts; the registry marks workers that miss their heartbeat budget unresponsive and then removes them. It is in-process by default, and `AddRedisWorkerRegistry`, `AddPostgresWorkerRegistry`, `AddSqlServerWorkerRegistry`, `AddMongoWorkerRegistry`, and `AddInMemoryWorkerRegistry` register one built on the storage primitives, which every host sharing that storage sees and which the same conformance tests hold to the same behavior on every provider
- Built-in data providers: queue statistics from the queue metrics store — whose waiting counts the brokers record as they publish, and whose consumers are counted from the worker registry — a task record store (written from task signals, expiring on a retention, and indexed by state and by task name) behind the task list, state counts, and task names, and metrics from the historical data store or computed live from the records, with a collector recording per-queue and per-task snapshots from the queue metrics and the per-task totals the signal handler keeps
- Task state changes and worker status changes are pushed over SignalR; the middleware passes API and hub paths through to their endpoints

### Observability and Release Verification
- Client and worker metrics record sends, receives, completions, retries, queue time, execution duration, and tasks in progress; OpenTelemetry registration uses the shared meter and tracing source
- CI checks the container-free suites, real-provider integration tests, dashboard lint/type/behavior/browser checks, and a published-and-run Native AOT sample
- Public API baselines track the shipped libraries; packaging checks the expected NuGet inventory and runs a consumer smoke test covering generated task registration, execution, and embedded dashboard assets
- Release publication is gated on integration tests and package verification, with provenance attestation and NuGet trusted publishing

### Documentation and Migration
- [Task tutorials, how-to guides, API references, and architecture explanations](docs/README.md)
- [Migration guides](docs/how-to/migrate-to-dotcelery.md) for Hangfire, Quartz.NET, Coravel, hosted services, and eligible message-bus operations, with a [compatibility reference](docs/reference/migration-compatibility.md)

## Known Gaps

### Delivery and Reliability
- Soft and hard time limits and revocation are cooperative; a task that ignores its cancellation token keeps its worker slot. Isolated process termination is not implemented
- Delivery and follow-up publication can repeat after a crash. Transactional result/inbox recording does not include arbitrary business effects or make broker publication atomic with storage writes

### Deployment and Compatibility Constraints
- Dashboard worker visibility and consumer counts are in-process by default; deployments with workers in other processes must register a worker registry on shared storage
- Beat coordination requires shared storage; without it every scheduler instance runs its schedule and optional file persistence is local. Missed-run handling runs an entry once rather than replaying every missed occurrence
- Beat entries evaluate five-field cron in UTC; richer formats and time-zone overloads in the standalone cron library do not extend Beat's scheduling contract
- Migration guides map concepts and application changes; no cross-framework stored-job importer or wire-format compatibility layer is supplied

## Planned Features

### Brokers
- Azure Service Bus broker, with lease renewal for long-running tasks, native delayed delivery, and explicit dead-letter versus discard; define its conformance-test environment and CI strategy before implementation

### Serialization
- Additional serializers (MessagePack/Protobuf), including pluggable broker envelopes; task payloads already use `IMessageSerializer`, while broker envelopes currently use JSON
- Message compression

### Task Registration and Dispatch
- Interceptors to reduce dispatch overhead
- Extension members for fluent task signatures, beyond the existing generated typed signature helpers

### Worker/Execution
- Connection pooling controls for brokers: separate publish and consume connections with channel pooling for RabbitMQ, which currently separates channels on one connection
- Batch execution tasks (one invocation processes an input batch), beyond the existing tracking and completion of batches of individual tasks

### CLI
- `dotcelery` command-line tool (worker, beat, inspect, task management, queue ops)
