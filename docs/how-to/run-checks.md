# Run project checks

[How-to guides](README.md) / Run project checks

Use this guide to verify a repository change. You need the .NET 10 SDK; source
builds that include the dashboard also need Node.js 24.3 or later. Docker is
required for the container integration suite.

## Build the current configuration

From the repository root:

```bash
dotnet tool restore
dotnet restore DotCelery.slnx
dotnet build DotCelery.slnx --configuration Release --no-restore
dotnet csharpier check .
```

## Run the container-free suites

Use `--no-build` only after a successful build of the same configuration:

```bash
dotnet test tests/DotCelery.Tests.Unit --configuration Release --no-build
dotnet test tests/DotCelery.Tests.Generated --configuration Release --no-build
dotnet test tests/DotCelery.Analyzers.Tests --configuration Release --no-build
```

For a dashboard-only investigation, narrow the unit suite with
`--filter FullyQualifiedName~Dashboard`. That filter does not replace the wider
checks required when shared runtime behavior changes.

## Verify frontend behavior

```bash
npm ci
npm run check
npx playwright install chromium
npm run test:browser
```

Keep port 5099 free for the browser suite's own backend. See
[Develop the dashboard](develop-dashboard.md) for hot reload and browser artifacts.

## Exercise real providers when affected

With Docker running:

```bash
dotnet test tests/DotCelery.Tests.Integration --configuration Release --no-build
```

Run .NET suites sequentially to avoid unnecessary contention with container
startup. SQL Server images require x86-64 emulation on ARM hosts. The conformance
project contains shared cases used by other suites; it is not run directly.

## Verify packages or Native AOT when affected

For package changes, after the Release build:

```bash
dotnet pack DotCelery.slnx --configuration Release --no-build --output artifacts/packages
./eng/verify-packages.sh artifacts/packages
```

For changes to AOT-compatible libraries, `just aot-smoke` publishes and runs the
existing native sample; it requires the platform's native build tools. See the
[Native AOT guidance](../../CONTRIBUTING.md#native-aot-and-trimming).

For required conventions and PR expectations, use
[Contributing](../../CONTRIBUTING.md). CI definitions are under
[`.github/workflows/`](../../.github/workflows).
