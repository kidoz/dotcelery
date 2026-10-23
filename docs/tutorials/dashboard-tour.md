# Explore the dashboard

[Tutorials](README.md) / Explore the dashboard

In this lesson you will inspect work produced by the dashboard demo and read a
REST API response.

## Before you begin

Use a DotCelery checkout with the .NET 10 SDK and Node.js 24.3 or later. The demo
uses in-memory services and generates its own traffic. Keep port 5080 available.

## 1. Start the demo

From the repository root, run:

```bash
dotnet run --project samples/DotCelery.Dashboard.Demo --no-launch-profile -- --urls http://localhost:5080 --Dashboard:ReadOnly=true
```

The first source build restores the frontend dependencies and compiles the
embedded UI. Leave this process running.

## 2. Observe the overview

Open <http://localhost:5080/celery/>. The header shows read-only access. Wait for
the first refresh; the worker summary should contain `demo-worker-1`, and recent
activity will fill with generated tasks.

The completion chart may initially be empty. Keep the page open for a metrics
snapshot; this demo records them every 15 seconds.

## 3. Inspect a task

Open **Tasks**, enter `demo.email.send` in the search field, and select **Search**.
Open a task name to see its ID, state, timing, and retry count. Close the details
dialog to resume automatic updates.

Copy the task link, open it in a new tab, and confirm that it opens the same task
details. Because this lesson uses read-only mode, retry and cancellation controls
are unavailable.

## 4. Read a capability response

In a second terminal, run:

```bash
curl --fail http://localhost:5080/celery/api/v1/capabilities
```

The JSON should contain `"readOnly":true` and `"refreshSeconds":3`. These values
describe the demo's settings. The version is part of the URL; the unversioned
`/celery/api/capabilities` path returns 404.

## 5. Stop the demo

Press **Ctrl+C** in the first terminal. Its task history disappears with the
in-memory process.

For your own host, follow [Host the dashboard](../how-to/host-dashboard.md).
For individual fields, use [Dashboard API v1](../reference/dashboard-api.md).
