# Move message-bus background work to DotCelery

[Migration guides](migrate-to-dotcelery.md) / MassTransit and NServiceBus

Use this guide to move an eligible background command's business operation from
a MassTransit consumer or NServiceBus handler into a DotCelery task. Inventory
the endpoint's other messaging responsibilities before choosing that boundary.

MassTransit consumers receive a typed message and consumer context; NServiceBus
handlers receive a typed message and handler context. Those contexts participate
in the source runtime's processing pipeline. See
[MassTransit consumers](https://masstransit.massient.com/concepts/consumers) and
[NServiceBus handlers](https://docs.particular.net/nservicebus/handlers/).

## 1. Classify the message flow

| Existing behavior                                               | Migration decision                                                                                              |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| One command invokes one bounded business operation              | Candidate for an explicit task wrapper                                                                          |
| An event has multiple independent subscribers                   | Preserve the required subscription and fan-out behavior                                                         |
| A consumer sends replies or publishes follow-up events          | Preserve those contracts and their delivery guarantees explicitly                                               |
| An active saga or routing workflow coordinates messages         | Keep its runtime owner until state, correlation, timers, and outgoing messages have a verified migration design |
| Processing uses a transactional outbox or transport transaction | Prove the destination transaction boundary before switching                                                     |

A task submission is directed background work. It does not reproduce a bus's
publish/subscribe or request/reply contract just because both use a broker. See
[MassTransit producers](https://masstransit.massient.com/concepts/producers) for
the source distinction between send and publish.

## 2. Keep the business service independent

Use the [common task wrapper](migrate-to-dotcelery.md#2-introduce-an-explicit-task-boundary)
and input DTO. Move business values out of `ConsumeContext<T>` or
`IMessageHandlerContext`, and keep source message IDs, correlation IDs, tenant
identity, and the business operation key in the migration mapping.

Keep the bus handler for work still owned by the bus. Where an adapter submits a
DotCelery task, specify exactly when the source message becomes acknowledged and
what durable evidence records the transfer.

## 3. Design the handoff failure window

Publishing the task and acknowledging the original message are normally separate
operations. A process can stop after publishing but before acknowledging, causing
the transfer to repeat. Acknowledging first can lose the work if publication then
fails.

Use a durable transfer/outbox design and enforce business idempotency for that
window. Do not present a direct `await client.SendAsync(...)` call inside a
handler as an atomic cross-framework handoff. DotCelery's transactional result
and inbox recording does not enlist an unrelated bus acknowledgement or arbitrary
business effects automatically.

For the source semantics, consult [NServiceBus recoverability](https://docs.particular.net/nservicebus/recoverability/)
and [Outbox](https://docs.particular.net/nservicebus/outbox/).
For the destination boundary, read [Delivery and retries](../explanation/delivery-and-retries.md).

## 4. Keep message formats and namespaces separate

Provision separate destination queues and storage namespaces. Even with the same
RabbitMQ, Redis, SQS, or database deployment, source messages and records are not
DotCelery `TaskMessage` envelopes. Use an application-level translator that
publishes through `ICeleryClient`; keep source consumers away from destination
queues and DotCelery workers away from raw source queues.

Retain active saga instances in the source runtime unless you have an explicit
state-transfer design. Similar names for saga, outbox, retry, or correlation
features do not establish compatible persistence or ordering semantics.

## 5. Verify and switch one command family

Exercise duplicate delivery, a failed transfer, worker failure, business-effect
idempotency, outgoing messages, tenant isolation, and correlation tracing. Ensure
you can identify one execution owner for each business operation during cutover.

Reconcile pending, scheduled, and error-queue work using source-supported
interfaces. Follow the [common migration procedure](migrate-to-dotcelery.md#6-cut-over-one-operation-family)
for cutover and rollback, retaining source history and workflow state until their
ownership is resolved.
