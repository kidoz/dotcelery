# Task contracts

[Reference](README.md) / Task contracts

## Task interfaces

The interfaces live in `DotCelery.Abstractions` under the
`DotCelery.Core.Abstractions` namespace.

| Interface                | Contract                                                                       |
| ------------------------ | ------------------------------------------------------------------------------ |
| `ITask`                  | Declares `static abstract string TaskName`                                     |
| `ITask<TInput>`          | Returns `Task` from `ExecuteAsync(input, context, cancellationToken)`          |
| `ITask<TInput, TOutput>` | Returns `Task<TOutput>` from `ExecuteAsync(input, context, cancellationToken)` |

Input and output generic parameters are reference types. Inputs and outputs must
be serializable by the configured serializer. `ITaskContext` supplies execution
information and operations such as requesting a retry.

Source: [ITask.cs](../../src/DotCelery.Abstractions/Abstractions/ITask.cs) and
[ITaskContext.cs](../../src/DotCelery.Abstractions/Abstractions/ITaskContext.cs).

## Registration and submission

`AddTask<TTask>()` registers a task explicitly. With the analyzer package installed,
`RegisterTasks()` in `DotCelery.Generated` registers generated task metadata and
typed invokers for the assembly's tasks. The generator is the registration path
used by the Native AOT sample.

`ICeleryClient.SendAsync<TTask, TInput, TOutput>(input, options, cancellationToken)`
returns an `AsyncResult<TOutput>` through a `ValueTask`. Its `GetAsync` operation
waits for the result with an optional timeout. A task ID identifies a submission;
the task name identifies the registered implementation.

Source: [ICeleryClient.cs](../../src/DotCelery.Client/ICeleryClient.cs),
[analyzer documentation](../../src/DotCelery.Analyzers/README.md), and
[AOT sample](../../samples/DotCelery.AotSmoke/Program.cs).

## States

| State      | Meaning                       | Terminal |
| ---------- | ----------------------------- | -------- |
| `Pending`  | Waiting for execution         | No       |
| `Received` | Received by a worker          | No       |
| `Started`  | Execution has started         | No       |
| `Success`  | Completed successfully        | Yes      |
| `Failure`  | Failed                        | Yes      |
| `Revoked`  | Revoked or cancelled          | Yes      |
| `Rejected` | Rejected without retry        | Yes      |
| `Retry`    | Waiting for a retry           | No       |
| `Progress` | Reports execution progress    | No       |
| `Requeued` | Returned for later processing | No       |

The state validator allows these changes, in addition to idempotent same-state
updates:

| Current state  | Allowed next states                                                          |
| -------------- | ---------------------------------------------------------------------------- |
| No state       | `Pending`, `Received`                                                        |
| `Pending`      | `Received`, `Revoked`                                                        |
| `Received`     | `Started`, `Revoked`                                                         |
| `Started`      | `Success`, `Failure`, `Retry`, `Revoked`, `Rejected`, `Requeued`, `Progress` |
| `Retry`        | `Received`, `Failure`, `Revoked`, `Rejected`                                 |
| `Requeued`     | `Received`, `Revoked`                                                        |
| `Progress`     | `Progress`, `Success`, `Failure`, `Revoked`, `Rejected`                      |
| Terminal state | No different state                                                           |

Source: [TaskState.cs](../../src/DotCelery.Abstractions/Models/TaskState.cs) and
[TaskStateValidator.cs](../../src/DotCelery.Abstractions/Models/TaskStateValidator.cs).

For a complete example, follow [Run your first task](../tutorials/first-task.md).
For redelivery and repeated effects, read [Delivery and retries](../explanation/delivery-and-retries.md).
