# DotCelery documentation

DotCelery runs background tasks in .NET applications. Start with a lesson, follow a
guide for a specific job, look up a contract, or read about the design.

The documentation follows [Diátaxis](https://diataxis.fr/start-here/): each section
serves a different reader need. These pages describe the checked-out source;
consult the [changelog](../CHANGELOG.md) when using a released package.

| What you need                       | Start here                           |
| ----------------------------------- | ------------------------------------ |
| Learn by building something         | [Tutorials](tutorials/README.md)     |
| Complete a specific task            | [How-to guides](how-to/README.md)    |
| Look up an API, option, or package  | [Reference](reference/README.md)     |
| Understand the design and tradeoffs | [Explanation](explanation/README.md) |

## First steps

1. [Run your first task](tutorials/first-task.md) with an in-memory client and worker.
2. [Explore the dashboard](tutorials/dashboard-tour.md) and inspect task history.
3. [Use Redis and PostgreSQL](how-to/use-redis-postgres.md) with the existing sample.

Moving an existing application? Start with
[Migrate background work to DotCelery](how-to/migrate-to-dotcelery.md) and the
[compatibility reference](reference/migration-compatibility.md).

## Find a topic

| Topic                                 | Documentation                                                   |
| ------------------------------------- | --------------------------------------------------------------- |
| Packages and supported providers      | [Package reference](reference/packages.md)                      |
| Task interfaces and lifecycle         | [Task contracts](reference/task-contracts.md)                   |
| Hosting and authorizing the dashboard | [Host the dashboard](how-to/host-dashboard.md)                  |
| REST v1 endpoints and payloads        | [Dashboard API](reference/dashboard-api.md)                     |
| Dashboard configuration defaults      | [Dashboard options](reference/dashboard-options.md)             |
| Dashboard hot reload and checks       | [Develop the dashboard](how-to/develop-dashboard.md)            |
| Execution and redelivery              | [Delivery and retries](explanation/delivery-and-retries.md)     |
| Frontend modules and refresh behavior | [Dashboard architecture](explanation/dashboard-architecture.md) |
| Repository architecture               | [System architecture](explanation/architecture.md)              |
| Building and testing changes          | [Run project checks](how-to/run-checks.md)                      |

Existing material on [Canvas workflows](../README.md#canvas-workflows),
[sagas](../README.md#saga-state-machine), [Beat scheduling](../README.md#beat-scheduler),
and [telemetry](../README.md#observability) remains available in the project README.
The [roadmap](../ROADMAP.md) records known gaps and planned work.

For development conventions, see [Contributing](../CONTRIBUTING.md). To improve
these pages, follow [Add a documentation page](how-to/write-documentation.md).
