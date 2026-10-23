# Packages

[Reference](README.md) / Packages

DotCelery targets .NET 10 and C# 14. Packages are split by responsibility so a
host can choose its transport and persistence providers.

| Package                       | Responsibility                                                                         |
| ----------------------------- | -------------------------------------------------------------------------------------- |
| `DotCelery.Abstractions`      | Interfaces, models, options, and shared JSON contracts                                 |
| `DotCelery.Core`              | Core registration, serialization, storage primitives, and shared store implementations |
| `DotCelery.Client`            | Task submission, results, and Canvas clients                                           |
| `DotCelery.Worker`            | Consumption, execution, filters, retries, and orchestration support                    |
| `DotCelery.Analyzers`         | Task-name diagnostics and generated registration and invokers                          |
| `DotCelery.Broker.InMemory`   | In-process message broker                                                              |
| `DotCelery.Broker.RabbitMQ`   | RabbitMQ transport                                                                     |
| `DotCelery.Broker.Redis`      | Redis Streams transport                                                                |
| `DotCelery.Broker.Sqs`        | Amazon SQS transport                                                                   |
| `DotCelery.Backend.InMemory`  | In-process result backend and storage                                                  |
| `DotCelery.Backend.Redis`     | Redis result backend and storage                                                       |
| `DotCelery.Backend.Postgres`  | PostgreSQL result backend and storage                                                  |
| `DotCelery.Backend.SqlServer` | SQL Server result backend and storage                                                  |
| `DotCelery.Backend.Mongo`     | MongoDB result backend and storage                                                     |
| `DotCelery.Storage.Sql`       | Shared SQL schema operations and migrations                                            |
| `DotCelery.Beat`              | Periodic task scheduling                                                               |
| `DotCelery.Cron`              | Cron expression support                                                                |
| `DotCelery.Dashboard`         | ASP.NET Core monitoring API, hub, and embedded React UI                                |
| `DotCelery.Telemetry`         | OpenTelemetry metrics and tracing                                                      |

`DotCelery.Abstractions` retains namespaces such as `DotCelery.Core.Abstractions`
and `DotCelery.Core.Models`; the assembly name does not imply a namespace rename.

## Provider registration names

| Responsibility     | Registration         |
| ------------------ | -------------------- |
| In-memory broker   | `UseInMemoryBroker`  |
| RabbitMQ broker    | `UseRabbitMQ`        |
| Redis broker       | `UseRedisBroker`     |
| SQS broker         | `UseSqs`             |
| In-memory results  | `UseInMemoryBackend` |
| Redis results      | `UseRedis`           |
| PostgreSQL results | `UsePostgres`        |
| SQL Server results | `UseSqlServer`       |
| MongoDB results    | `UseMongo`           |

Additional storage-backed features have explicit registrations such as
`AddPostgresOutboxStore` and `AddRedisWorkerRegistry`. Choosing a result backend
does not mean every optional feature has been registered.

## Source builds and runtime requirements

- The solution uses the .NET 10 SDK. Source builds including the dashboard need
  Node.js 24.3 or later to generate embedded assets.
- Applications consuming the compiled dashboard package do not need Node.js.
- Container integration tests require Docker.
- Native AOT verification covers Abstractions, Core, Client, Worker, and the
  in-memory broker and backend. Driver-based providers and the dashboard are
  outside that verification boundary.

The package inventory is [eng/packages.txt](../../eng/packages.txt). Each project
under [src/](../../src) owns its package dependencies and public API baselines.
See the [roadmap](../../ROADMAP.md) for planned providers.
