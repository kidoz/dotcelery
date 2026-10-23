# Dashboard REST API v1

[Reference](README.md) / Dashboard API

The API base URL is `{prefix}/api/v1`, where the default prefix is `/celery`.
The hub is separate at `{prefix}/hub`.

## Versioning and discovery

The controller declares API version 1.0 through `Asp.Versioning.Mvc`. Requests
must include the URL version. Unversioned `/api/*` routes, unsupported versions,
and malformed version segments return 404. Responses from v1 endpoints advertise
`api-supported-versions`.

Version-aware API Explorer substitutes concrete versioned URLs and exposes the
`v1` group for host OpenAPI integration. The host owns any documentation endpoint.
API versions are independent of NuGet package versions. Compatible optional
fields can remain in v1; breaking contracts require a new API version.

These are shared MVC versioning services. Host controllers should declare their
versions or use `[ApiVersionNeutral]` when unversioned. Applications already
configuring API versioning should configure shared options after adding the
dashboard and retain a URL-segment reader for its routes.

## Access and errors

Dashboard authorization covers API endpoints as well as the page and hub. All
POST requests, including action previews, require `X-DotCelery-Dashboard: 1`
when `VerifyRequestOrigin` is enabled. If an `Origin` header is present, it must
match the request's origin or a configured allowed origin.

| Status | Typical cause                                                                                  |
| ------ | ---------------------------------------------------------------------------------------------- |
| `400`  | Invalid query/body, invalid time range, missing operation store, or too many explicit task IDs |
| `403`  | Authorization denied, failed request verification, or forbidden mutation                       |
| `404`  | Missing task or unsupported route/version                                                      |

Model-binding validation errors use Problem Details. Some controller errors have
simpler bodies; check the status before attempting to parse a JSON payload.
Mutations may have taken effect even when a client loses the response.

## Endpoints

Paths below are relative to `{prefix}/api/v1`.

| Method | Path                     | Response or operation                                                    |
| ------ | ------------------------ | ------------------------------------------------------------------------ |
| GET    | `/capabilities`          | Effective permissions, available stores, retention, and refresh settings |
| GET    | `/overview`              | State counts, worker counts, queue gauges, and default-window metrics    |
| GET    | `/queues`                | Queue statistics                                                         |
| GET    | `/workers`               | Registered workers                                                       |
| GET    | `/tasks/search`          | Combined-filter task page                                                |
| GET    | `/tasks`                 | Existing task listing with state/name and paging parameters              |
| GET    | `/tasks/{taskId}`        | Task summary, or 404                                                     |
| GET    | `/task-names`            | Available task names                                                     |
| GET    | `/metrics`               | Metrics for `windowMinutes`                                              |
| GET    | `/metrics/historical`    | Historical metrics query                                                 |
| GET    | `/metrics/timeseries`    | Completion buckets; accepts `from`, `until`, and `granularity`           |
| GET    | `/metrics/by-task`       | Per-task metrics query                                                   |
| POST   | `/tasks/actions/preview` | Eligible cancellation and retry IDs for an explicit selection            |
| POST   | `/tasks/{taskId}/revoke` | Request revocation of one task                                           |
| POST   | `/tasks/bulk/revoke`     | Request revocation of selected tasks                                     |
| POST   | `/tasks/bulk/retry`      | Requeue original messages found in the dead-letter store                 |
| POST   | `/tasks/filter`          | Preview a `TaskFilter` selection                                         |

The complete parameter and response declarations for the metrics queries are in
[DashboardApiController.cs](../../src/DotCelery.Dashboard/Controllers/DashboardApiController.cs).

## Capabilities

`GET /capabilities` returns:

| Field                          | Meaning                                            |
| ------------------------------ | -------------------------------------------------- |
| `readOnly`                     | Effective read-only mode                           |
| `canCancel`, `canRetry`        | Operations permitted by mode and configured stores |
| `hasHistory`, `hasTaskRecords` | Historical metrics and task history availability   |
| `retentionSeconds`             | Task record retention in seconds                   |
| `snapshotSeconds`              | Historical snapshot interval in seconds            |
| `refreshSeconds`               | Effective polling interval in seconds              |
| `metricsWindowMinutes`         | Overview's default metrics window in minutes       |
| `pageSize`                     | Effective maximum page size                        |

## Task search

All supplied criteria are combined by `GET /tasks/search`.

| Parameter     | Default | Meaning                                                               |
| ------------- | ------- | --------------------------------------------------------------------- |
| `state`       | Unset   | Exact task state name                                                 |
| `taskName`    | Unset   | Exact task name; at most 500 characters                               |
| `search`      | Unset   | Case-insensitive substring of task ID or name; at most 500 characters |
| `queue`       | Unset   | Exact queue; at most 500 characters                                   |
| `worker`      | Unset   | Exact worker; at most 500 characters                                  |
| `from`        | Unset   | Inclusive earliest update timestamp                                   |
| `until`       | Unset   | Exclusive latest update timestamp                                     |
| `oldestFirst` | `false` | Ascending update order                                                |
| `page`        | `1`     | One-based page, clamped to at least 1                                 |
| `pageSize`    | `25`    | Requested rows, bounded by the configured cap                         |

The update timestamp is `completedAt`, otherwise `startedAt`, otherwise `sentAt`.
A supplied `from` must precede `until`. The response has `tasks`, `page`,
`pageSize`, and `hasMore`; it does not include a total count. Task summaries use
string state names and TimeSpan strings for durations.

## Task operations

Preview an explicit selection with this body:

```json
{ "taskIds": ["task-id-1", "task-id-2"] }
```

The preview returns `cancelTaskIds` and `retryTaskIds`. It is advisory: eligibility
can change before the mutation. A failed task record alone does not make retry
available; the original message must be retained in the dead-letter store.

`POST /tasks/bulk/retry` accepts `taskIds`, an optional `filter`, and an optional
`targetQueue`. `POST /tasks/bulk/revoke` accepts `taskIds`, an optional `filter`,
`terminate`, and `immediate`. Prefer explicit previewed IDs for interactive actions.

Bulk results include `processedCount`, `successCount`, `failureCount`, optional
`failedTaskIds`, and an optional `message`. Check partial failures. Cancellation
success means the request was accepted; a running task can still finish.

Explicit ID lists exceeding `MaxBulkOperationSize` are rejected. Filter-selected
operations are bounded. `TaskFilter` uses `state`, `taskNamePattern`, `queue`,
`completedAfter`, and `completedBefore`; it is distinct from task search parameters.

Source: [DashboardQueries.cs](../../src/DotCelery.Dashboard/Controllers/DashboardQueries.cs),
[TaskSearchRequest.cs](../../src/DotCelery.Dashboard/Models/TaskSearchRequest.cs),
[bulk operation models](../../src/DotCelery.Dashboard/Models/BulkOperationModels.cs), and
[endpoint tests](../../tests/DotCelery.Tests.Unit/Dashboard/DashboardEndpointSecurityTests.cs).
