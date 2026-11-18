# Rotate message signing keys

[How-to guides](README.md) / Rotate message signing keys

Configure message signing with `DotCeleryBuilder.UseMessageSecurity`. It is
available from `DotCelery.Core` in worker, client-only, Beat, and dashboard hosts.
Use `MessageSecurityOptions.GenerateKey()` to make a 32-byte key and load it from
the host's secret store. Keep keys independent for each authenticated tenant.

1. Upgrade all publishers and consumers while keeping their current key active.
   Leave `SigningKeyId` unset while any older consumer still receives deliveries.
2. Add the new key and its ID to `AdditionalVerificationKeys` on every consumer,
   including any application that also produces work. Tenants configure their
   own entries under `TenantAdditionalVerificationKeys`.
3. Switch publishers to the new key and set `SigningKeyId`. Retain the old key
   under its ID in `AdditionalVerificationKeys`. If older queued messages carry
   no ID, set `LegacyVerificationKeyId` to the ID of the old key. Tenant hosts
   set the new active keys and IDs in `TenantSigningKeys` and `TenantSigningKeyIds`.
   In that mode each tenant resolves legacy IDs in its own keys.
4. Keep verification overlap until all messages signed with the old key have
   expired or been consumed, including delayed deliveries, retries, and any
   dead letters that will be replayed. Account for message age and clock skew
   when computing the overlap. Remove the old verification key and clear the
   legacy ID when legacy deliveries have drained.

A host with `EnableMessageSigning = false` can verify signed messages without
signing its own publications by setting `VerifySignaturesWhenPresent = true`
and supplying the verification keys. That setting accepts unsigned deliveries.
Authenticated tenant isolation requires signing and rejection of unsigned
deliveries. When both signing and verification are off, the default validator
accepts signed deliveries as unverified and logs once when it sees one.
