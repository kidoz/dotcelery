# DotCelery Redis/Postgres Example

This sample runs a DotCelery client and worker in one process against real local services:

- Redis Streams as the message broker
- PostgreSQL as the default result backend
- Redis as an alternate result backend

Start the dependencies from the repository root:

```bash
docker compose up -d redis postgres
```

Run with Redis broker and PostgreSQL result storage:

```bash
dotnet run --project samples/DotCelery.RedisPostgresExample
```

Run with Redis broker and Redis result storage:

```bash
dotnet run --project samples/DotCelery.RedisPostgresExample -- --result-backend=redis
```

Override connection strings when needed:

```bash
DOTCELERY_REDIS_CONNECTION_STRING=localhost:6380 \
DOTCELERY_POSTGRES_CONNECTION_STRING="Host=localhost;Port=5433;Database=dotcelery;Username=dotcelery;Password=dotcelery" \
dotnet run --project samples/DotCelery.RedisPostgresExample
```

Stop the dependencies:

```bash
docker compose down
```
