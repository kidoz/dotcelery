# Dashboard architecture

[Explanation](README.md) / Dashboard architecture

The dashboard is a React application hosted by an ASP.NET Core library. Vite
produces static JavaScript, CSS, HTML, and image assets; the .NET assembly embeds
those outputs. A consumer therefore runs its normal ASP.NET Core host without a
frontend server or Node.js runtime.

## Runtime paths are a host decision

The host chooses `PathPrefix`, so a production bundle cannot assume `/celery` at
build time. The generated HTML uses relative asset URLs and a runtime-injected
base path. The middleware encodes the title and prefix and serves explicitly named
resources, preserving dots and casing in hashed asset names.

This supports a slashless mount URL and nested UI paths with the same bundle.
API and hub paths pass through to endpoint routing instead of receiving the SPA
fallback page.

## Module boundaries follow responsibilities

The source under `Frontend/src/` is organized as:

| Directory                               | Responsibility                                                                 |
| --------------------------------------- | ------------------------------------------------------------------------------ |
| `app/`                                  | Application composition, routing, request planning, polling, and global styles |
| `features/overview/`                    | Overview and completion chart                                                  |
| `features/tasks/`                       | Task search, selection, query construction, and task dialogs                   |
| `features/queues/`, `features/workers/` | Queue and worker views                                                         |
| `shared/api/`                           | REST v1 contracts, HTTP client, errors, and request sessions                   |
| `shared/domain/`, `shared/model/`       | Task states, filters, navigation, and shared data types                        |
| `shared/lib/`, `shared/ui/`             | Display formatting and reusable controls                                       |

The application composes features. Features use shared modules; shared code does
not depend on feature or application orchestration. ESLint enforces these import
boundaries. Direct imports make those relationships visible.

## Polling is scoped to the visible work

The bundled UI polls; the backend also exposes SignalR endpoints for other
clients. The presence of the hub does not mean this React UI uses it.

Queue and worker pages request their own lists. Task history requests its task
page at the configured interval and caches task names and filter lists for 60
seconds. The overview reuses queue gauges and matching-window metrics already
present in its overview response. It requests separate metrics for a different
window and historical buckets only when history is available.

Responses update independently, and one endpoint cannot overlap another request
to itself in the same polling session. A slow filter list does not hold back task
rows. Manual refresh bypasses the static-list cache.

## UI state must survive asynchronous work

The HTTP client adds request verification, preserves cancellation, and bounds
request duration. Polling pauses for explicit pause, hidden tabs, selection, and
dialogs. Its indicator reflects dialog-driven suspension too.

Task pages carry a filter fingerprint. After a filter change, old rows cannot be
selected while a different query is pending. Request sessions also prevent an
older response from replacing current data.

Mutation completion has a different lifetime from a dialog. A submitted POST can
finish after browser Back closes that dialog; the result may still be reported,
but action and navigation generations prevent it from replacing a newer route or
closing a newer confirmation. Leaving the dialog does not cancel a submitted
mutation and pretend it never reached the server.

Source: [frontend](../../src/DotCelery.Dashboard/Frontend/src),
[middleware](../../src/DotCelery.Dashboard/Middleware/DashboardMiddleware.cs),
[build integration](../../src/DotCelery.Dashboard/DotCelery.Dashboard.csproj), and
[browser regressions](../../eng/dashboard.browser.mts).

For commands, follow [Develop the dashboard](../how-to/develop-dashboard.md).
For wire contracts, use [Dashboard API v1](../reference/dashboard-api.md).
