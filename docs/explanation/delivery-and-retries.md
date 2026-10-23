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

## Recover publication after a crash

The built-in inbox stores the successful outcome, including the signal and
follow-up checkpoints, with its processed marker. That payload remains available
for `InboxRetention` (default seven days), extended to cover `MaxMessageAge` when
longer, even if the public result expires after the default one day. A duplicate
uses the stored payload without running the task body or inventing a new success.
The retained payload has the same sensitivity as a result; include inbox storage
in your data-retention policy. Checkpoint writes refresh its retention.

Workflow completion handlers are critical signals: a handler failure propagates
so a successful delivery can replay unfinished signals. Other observation
handlers remain best effort. Queued dispatch propagates a failed terminal task signal enqueue; its
processor retries critical handler failures under the signal store retry policy.
Monitor `signals.failed` and recover archived signals when that budget is
exhausted. Chord callbacks, saga steps, and compensations also
have recoverable publication claims in their durable documents. `AddBatchSupport`
and `AddSagaSupport` register a recovery service that scans pending publications.
Expired claims can be reclaimed after `StorageStoreOptions.ClaimTimeout`.

Each recovered publication reuses its task ID. Saga advancement atomically checks
that the named completed step is still current. A replay cannot advance a
different step, even if that later step has already completed. The handler and
recovery scanner use `ContinueStepAsync` with that identity; a crash after
recording completion but before advancement can still continue the same step.
A broker can accept publication before the checkpoint is saved; recovery can
therefore publish the same ID twice. Enable inbox deduplication on receiving
workers and design business effects to tolerate concurrent duplicate delivery.
This protocol does not provide exactly-once task effects.

Custom `IBatchStore` implementations must implement `CompleteCallbackAsync` and
`GetPendingCallbacksAsync`, and make callback claims reclaimable after timeout.
Custom `ISagaStore` implementations must implement `TryClaimStepDispatchAsync`
and `CompleteStepDispatchAsync`, persist task mappings before publication, and
implement `TryAdvanceStepAsync` with the expected step identity and completion
state checked inside the atomic update. It returns null on a stale or unfinished
step. Custom `ISagaOrchestrator` implementations must implement `ContinueStepAsync`
and carry that identity through to the store. The existing `ContinueAsync` is
for resuming the current completed step, rather than replaying a particular
completion event. Custom inbox stores can implement
`IInboxOutcomeStore`; a marker-only implementation cannot recover an expired
continuation result. A legacy marker with no continuation data is rejected and
sent through the dead-letter path instead of being retried indefinitely.

During upgrade, old callback claim timestamps cannot prove whether publication
happened. The new recovery service treats finished batches without
`CallbackPublished` as pending and may replay them after the claim expires. Old
callbacks with random IDs cannot be deduplicated against the new deterministic
ID. Reconcile or archive old finished batches before enabling recovery if such a
replay is unacceptable, and upgrade collaborating workers together.

## Preserve unreadable Redis messages

Redis moves an unreadable entry into an operator quarantine stream in the same
Lua operation that acknowledges and deletes the original. Empty, oversized,
invalidly signed, and malformed bodies retain the original fields and source
stream/entry ID. A failed archive write leaves the original pending.

`RedisBrokerOptions.DeadLetterStreamPrefix` defaults to
`dotcelery:dead-letter:`; the full quarantine key is that prefix plus the source
stream key. These streams are outside the dashboard dead-letter store and have
no automatic trimming. Operators must monitor, inspect, replay when appropriate,
and apply their own retention policy. The atomic move requires both streams to
be usable by the same Redis script; a cross-slot error leaves the source pending.

## Keep Beat occurrences across hosts

With registered storage, Beat keeps shared per-entry cursors and the serialized
pending occurrence in storage, reloads them on each leader tick, and renews its
leader lease while publishing. A pending occurrence is saved before publication
and cleared with a version check afterwards. A takeover resumes it with the same
message ID and payload. Ambiguous publication can still produce duplicate
messages with that ID. Without storage, `PersistState` continues to use a local
file, which does not coordinate hosts.

## Keep a submission ID when acceptance is uncertain

The client writes `Pending` before publishing. If that metadata write fails,
nothing is published. If publication fails, `TaskSubmissionException.TaskId`
exposes the assigned ID, including when broker acceptance is ambiguous. Inspect
that ID or explicitly reuse it in `SendOptions.TaskId` when retrying. Generating
a fresh ID creates a separate invocation. Callers needing to survive process
loss should allocate and persist their task ID before sending.

Source: [OutcomeRecorder](../../src/DotCelery.Core/Storage/Stores/OutcomeRecorder.cs),
[its tests](../../tests/DotCelery.Tests.Unit/Storage/OutcomeRecorderTests.cs),
[worker settlement](../../src/DotCelery.Worker/Execution/WorkerMessageSettler.cs),
[inbox filter](../../src/DotCelery.Worker/Filters/InboxDeduplicationFilter.cs), and
[storage options](../../src/DotCelery.Abstractions/Storage/Stores/StorageStoreOptions.cs).
