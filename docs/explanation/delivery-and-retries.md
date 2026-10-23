# Delivery and retries

[Explanation](README.md) / Delivery and retries

Background work can run again after a crash, a retry, or an uncertain response.
Understanding the boundary between execution, outcome recording, and message
settlement is necessary before relying on a delivery guarantee.

## An outcome and an acknowledgement are separate

The worker executes task code and records its outcome before the delivery is
settled. A failure between those steps can leave the broker able to deliver the
message again. The broker's capabilities and the registered storage features
determine how that repeat is handled.

With an inbox and a result backend sharing the same transactional storage
instance, `OutcomeRecorder` commits a successful result and its processed marker
together. A crash after that commit leaves evidence that inbox deduplication can
use. Without shared transactional storage, the result is stored first and the
inbox marker afterwards; a failure between those writes leaves a repeat possible.

This does not put arbitrary task side effects into the same transaction. Sending
an email or calling an external payment service may already have happened when a
worker crashes. Task effects need their own idempotency or transactional design.

## Retry is another execution

A retry policy chooses whether an exception is retryable and computes a delay.
A task attribute takes precedence over the worker default. A retry requested
without an explicit countdown can use that policy's backoff; automatic retries
also depend on the resilience configuration and remaining retry budget.

The `Retry` state is distinct from terminal `Failure`. Retrying a dead-letter
message through the dashboard is another way to run the business operation again;
the presence of a failed summary alone is insufficient to reconstruct its input.

See the existing [retry configuration examples](../../README.md#retry-policies)
for the registration syntax.

## Cancellation is cooperative

A revocation can prevent a pending task from starting. With termination requested,
a running task's cancellation token is signalled. A task that does not observe
the token can retain its slot or finish its operation.

Consequently, the dashboard reports an accepted cancellation request separately
from the task's eventual state. Terminal outcomes end the pending message even
when the task succeeded or failed rather than becoming revoked. Hard time limits
also remain cooperative, as recorded in the [known gaps](../../ROADMAP.md#delivery-and-reliability).

## Claims, retention, and uncertainty

Feature-store claims hide work such as outbox items for a bounded time. If a claim
expires after a consumer stops, another consumer may receive it. These claims are
separate from a broker's delivery and lease mechanisms.

Inbox and result retention determine how long evidence of a previous execution
remains available. Expired evidence cannot justify an unbounded deduplication
claim. A lost HTTP response similarly does not prove that a submitted mutation
failed; inspect current state before submitting the action again.

Source: [OutcomeRecorder](../../src/DotCelery.Core/Storage/Stores/OutcomeRecorder.cs),
[its tests](../../tests/DotCelery.Tests.Unit/Storage/OutcomeRecorderTests.cs),
[worker settlement](../../src/DotCelery.Worker/Execution/WorkerMessageSettler.cs),
[inbox filter](../../src/DotCelery.Worker/Filters/InboxDeduplicationFilter.cs), and
[storage options](../../src/DotCelery.Abstractions/Storage/Stores/StorageStoreOptions.cs).
