# Use Redis and PostgreSQL

[How-to guides](README.md) / Use Redis and PostgreSQL

Run the repository sample against Redis Streams and a PostgreSQL result backend.
You need the .NET 10 SDK, Docker with Compose, and free ports 6380 and 5433.
Run all commands from the repository root.

## Start the local services

```bash
docker compose up -d redis postgres
docker compose ps
```

Wait for both services to be healthy. The repository's [Compose file](../../compose.yaml)
maps Redis to `localhost:6380` and PostgreSQL to `localhost:5433`.

## Run the sample

```bash
dotnet run --project samples/DotCelery.RedisPostgresExample
```

The sample publishes a task to the `media` queue, runs a worker, and prints a
stored result with dimensions `1280x720`. Its default result backend is PostgreSQL.

To use Redis for results as well as transport:

```bash
dotnet run --project samples/DotCelery.RedisPostgresExample -- --result-backend=redis
```

## Use different connection settings

The sample reads these environment variables:

| Variable                               | Sample default                                                                      |
| -------------------------------------- | ----------------------------------------------------------------------------------- |
| `DOTCELERY_REDIS_CONNECTION_STRING`    | `localhost:6380`                                                                    |
| `DOTCELERY_POSTGRES_CONNECTION_STRING` | `Host=localhost;Port=5433;Database=dotcelery;Username=dotcelery;Password=dotcelery` |

Set them in the terminal or process environment before running the sample. The
credentials above belong to the local development Compose service.

## Stop the services

```bash
docker compose stop redis postgres
```

For application registration and other providers, see the
[sample implementation](../../samples/DotCelery.RedisPostgresExample/Program.cs)
and [package reference](../reference/packages.md). For migration and retention
details, see the existing [PostgreSQL storage material](../../README.md#postgresql-schema).
