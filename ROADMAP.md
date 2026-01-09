# Roadmap

This document tracks planned features and larger architectural work that are not yet implemented.
Status and scope may change as the project evolves.

## Completed Features

### Brokers
- Redis broker (Redis Streams with consumer groups)

### Serialization
- AOT-friendly serialization contexts (`DotCeleryJsonContext`, `RedisBackendJsonContext`)

## Planned Features

### Brokers
- Azure Service Bus broker
- Amazon SQS broker

### Backends
- SQL Server result backend

### Serialization
- Pluggable serializers (MessagePack/Protobuf)

### Task Registration and Dispatch
- Source generator for task registration and strongly-typed client helpers
- Interceptors to reduce dispatch overhead
- Extension members for fluent task signatures

### Worker/Execution
- Inbox-based deduplication wired into worker execution (exactly-once semantics)
- Connection pooling controls for brokers/backends
- Batch execution tasks (single-task processing of input batches)

### Security
- Message signing
- Serialization allowlists
- Message size limits and compression

### CLI
- `dotcelery` command-line tool (worker, beat, inspect, task management, queue ops)
