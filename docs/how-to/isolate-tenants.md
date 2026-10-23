# Isolate tenants on shared workers

[How-to guides](README.md) / Isolate tenants

Use independent message keys, authorized queues, and scoped storage together.
Setting `TenantId` or choosing a queue alone does not authenticate a tenant.
Shared workers are trusted infrastructure: they hold every configured tenant's
key and run task code in the same process. This feature does not sandbox task
code or isolate tenants from a compromised worker or database administrator.

## Configure identities and queues

On each worker and dashboard host, keep your existing durable broker, backend,
and feature-store registrations, then configure:

```csharp
using DotCelery.Core.MultiTenancy;
using DotCelery.Core.Security;

builder.Services.AddDotCelery(celery => celery
    .AddTenantRouting(options =>
    {
        options.Enabled = true;
        options.ValidTenants = ["acme", "contoso"];
    })
    .UseMessageSecurity(options =>
    {
        options.EnableMessageSigning = true;
        options.RejectUnsignedMessages = true;
        options.TenantSigningKeys["acme"] = acmeKey;
        options.TenantSigningKeys["contoso"] = contosoKey;
    }));

var app = builder.Build();
var router = app.Services.GetRequiredService<ITenantRouter>();
router.AddTenantQueue("acme", "jobs-acme");
router.AddTenantQueue("contoso", "jobs-contoso");
// Configure middleware, then start the host.
```

Load distinct random keys from your secret manager; use at least 32 bytes each.
`RequireAuthenticatedTenants` defaults to `true`. With tenancy enabled, startup
requires signing, rejection of unsigned messages, and per-tenant keys. Each
configured tenant needs an explicit, unique queue mapping before the host starts.
Configure the worker's `WorkerOptions.Queues` to consume those queues. Broker
credentials and queue ACLs remain the host's responsibility.

Give a tenant-owned publisher only its own key and route. A populated
`TenantSigningKeys` map never falls back to `SigningKey`. The worker checks the
explicit signed `TenantId`, the actual delivery queue, the envelope queue, and
any tenant header before accessing tenant data. A custom broker must supply the
original `RawBody` as well as the signature; the object must match those bytes.
Invalid deliveries reaching the worker are archived in the operator-only
`{StorageStoreOptions.Prefix}.quarantine` collection before acknowledgement.
Archival failure leaves the delivery recoverable. Broker-level unreadable bodies
use the transport's own rejection/archive path.

## Bind trusted application operations

Resolve a tenant from your application's authenticated identity and authorize it
before entering a scope. Do not take this value directly from an HTTP header,
query parameter, or task argument.

```csharp
using var scope = TenantContext.SetTenant(
    authenticatedTenantId,
    router.GetQueue(authenticatedTenantId));

// Send tasks, start a canvas/batch/saga, read results, or request revocation here.
```

`CeleryClient` uses the ambient tenant and binds its authorized queue. It rejects
an attempt to override that tenant or route in send options. The two-argument
scope also binds the route for canvas, batch, and saga publication. Worker
continuations and tenant background services establish that scope automatically.

Built-in stores use tenant-specific document collections, queues, lease keys,
counters, and notification channels. Access without an ambient tenant fails
closed. Identical task IDs in different tenants refer to different records.
Each tenant has independent workflow recovery, delayed/outbox/signal dispatch,
heartbeats, metrics, and revocations. Custom store implementations must implement
the same isolation contract; registering them does not automatically wrap their
own persistence. Direct access to `IStorageProvider` remains an administrative
primitive API.

## Scope dashboard requests

Keep the dashboard authorization callback and host authentication middleware.
Issue one trusted `dotcelery:tenant` claim in the authenticated user's principal.
`DashboardOptions.TenantIdClaimType` changes that claim name. API requests and hub
connections require a known authenticated tenant independently of the access
callback; a header cannot override it. Queries, mutations, metrics, and SignalR
groups use that tenant. There is no cross-tenant dashboard administrator mode.

## Upgrade existing deployments

Enabled tenancy now requires these settings by default. For trusted applications
that deliberately need only legacy metadata routing, set
`RequireAuthenticatedTenants = false`; that setting does not provide an isolation
boundary.

Existing unscoped data is not assigned to a tenant automatically. Stop the old
writers, explicitly migrate owned records into their tenant namespaces, and
switch readers and writers together. Do not run scoped and unscoped hosts against
one logical workload during migration. The physical tenant prefix consumes 67
characters of the 512-character lease/counter key budget; collection, queue, and
channel names are hashed within their 128-character limit.

Shared Beat cursor records and the operator quarantine are administrative data,
outside tenant query stores. Protect database and broker access accordingly.
See [delivery recovery](../explanation/delivery-and-retries.md) for publication
and upgrade boundaries.
