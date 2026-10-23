# Dashboard options

[Reference](README.md) / Dashboard options

`DotCelery.Dashboard.DashboardOptions` is configured through
`AddDotCeleryDashboard(options => ...)`.

| Property                    | Default               | Effect                                                                                         |
| --------------------------- | --------------------- | ---------------------------------------------------------------------------------------------- |
| `PathPrefix`                | `/celery`             | Mount prefix for UI, REST API, and hub                                                         |
| `Title`                     | `DotCelery Dashboard` | Title injected into the page                                                                   |
| `RequireAuthorization`      | `true`                | Requires an authorization callback that grants access                                          |
| `AuthorizationCallback`     | `null`                | `Func<HttpContext, Task<bool>>` deciding access                                                |
| `TenantIdClaimType`         | `dotcelery:tenant`     | Authenticated principal claim used when tenant isolation is enabled                             |
| `ReadOnly`                  | `false`               | Disables task mutations when true                                                              |
| `AllowTaskOperations`       | `true`                | Allows task mutations when stores and permissions also permit them                             |
| `VerifyRequestOrigin`       | `true`                | Requires the dashboard header on state-changing API requests and validates any supplied origin |
| `AllowedOrigins`            | Empty                 | Additional permitted origins                                                                   |
| `RefreshIntervalSeconds`    | `5`                   | Frontend refresh interval; capabilities report at least one second                             |
| `PageSize`                  | `50`                  | Maximum requested page size; the effective cap is bounded to 1–1000                            |
| `MaxBulkOperationSize`      | `1000`                | Maximum explicit IDs in a bulk request and bound for filter-selected operations                |
| `MetricsWindow`             | One hour              | Default metrics window used by the overview endpoint                                           |
| `TaskRecordRetention`       | 24 hours              | Retention of dashboard task records                                                            |
| `WorkerOfflineTimeout`      | 90 seconds            | Heartbeat budget before a worker becomes unresponsive                                          |
| `MetricsSnapshotInterval`   | One minute            | Historical snapshot interval when a historical store is registered                             |
| `ExposeExceptionDetails`    | `false`               | Controls whether full exception messages are exposed                                           |
| `MaxExceptionMessageLength` | `200`                 | Bound on displayed exception message length                                                    |
| `EnableRealTimeUpdates`     | `true`                | Exposed SignalR setting; the bundled frontend currently uses polling                           |

`ReadOnly || !AllowTaskOperations` makes the effective API access mode read-only.
Cancellation additionally needs a revocation store; retry additionally needs a
dead-letter store. The capability response reports those effective conditions.

With `RequireAuthorization = true`, a missing callback denies access. A callback
that is configured still runs when `RequireAuthorization = false`.

The normalized prefix gains a leading slash and loses trailing slashes. Use the
same prefix in registration and endpoint mapping. Titles and prefixes are HTML
encoded when injected into the embedded page.

`WorkerOfflineTimeout` and storage `WorkerRetention` are different settings:
the former is the dashboard's liveness budget; the latter controls how long the
storage-backed registry retains a heartbeat record. Keep their relationship to
the worker heartbeat interval consistent across hosts.

Source: [DashboardOptions.cs](../../src/DotCelery.Dashboard/DashboardOptions.cs),
[capability calculation](../../src/DotCelery.Dashboard/Controllers/DashboardQueries.cs),
[authorization](../../src/DotCelery.Dashboard/Security/DashboardAuthorization.cs), and
[storage options](../../src/DotCelery.Abstractions/Storage/Stores/StorageStoreOptions.cs).

For setup steps, follow [Host the dashboard](../how-to/host-dashboard.md).
