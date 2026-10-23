# Develop the dashboard with hot reload

[How-to guides](README.md) / Develop the dashboard

Use a DotCelery checkout with the .NET 10 SDK and Node.js 24.3 or later. Run the
commands below from the repository root.

## Restore the frontend tools

```bash
npm ci
```

## Start the backend and Vite

Run the demo backend in one terminal:

```bash
dotnet run --project samples/DotCelery.Dashboard.Demo --no-launch-profile -- --urls http://localhost:5099
```

In a second terminal:

```bash
npm run dev
```

Open Vite's displayed local URL. The development shell uses `/celery` and proxies
its API and hub requests to `http://localhost:5099`. Set
`DOTCELERY_DASHBOARD_BACKEND` in Vite's environment to change the backend address.
Use a backend configured with the `/celery` prefix for this development shell.

The proxy preserves the request host so the backend's same-origin verification
continues to apply to task actions.

## Make and check the change

Edit the relevant feature under
[`Frontend/src/features/`](../../src/DotCelery.Dashboard/Frontend/src/features).
Use shared UI controls and API contracts when the behavior crosses features.
For dependency boundaries and polling, read
[Dashboard architecture](../explanation/dashboard-architecture.md).

```bash
npm run check
npm run build
```

`check` runs ESLint, Prettier, both TypeScript projects, and the Node.js logic
tests. `build` creates production assets under
`src/DotCelery.Dashboard/obj/dashboard/`. A .NET source build also builds and
embeds these assets automatically.

Apply formatting or automatic lint fixes with:

```bash
npm run format
npm run lint:fix
```

## Exercise the compiled UI

Stop the development backend on port 5099, then run:

```bash
npx playwright install chromium
npm run test:browser
```

Playwright starts its own demo backend on port 5099 with `/jobs/monitor` as the
prefix. These checks exercise the embedded production assets, not Vite's dev
server. Traces and failure screenshots are written under `test-results/`.

When API paths, authorization, or embedding change, also run the relevant
[.NET checks](run-checks.md). Configuration lives in
[`vite.config.mts`](../../vite.config.mts),
[`eslint.config.mts`](../../eslint.config.mts), and
[`prettier.config.mts`](../../prettier.config.mts). Keep TypeScript within the
range supported by the pinned `typescript-eslint` dependency when upgrading tools.
