# tls: support dynamic TLS profile configuration via XDS

Fixes #<ISSUE_NUMBER>

## What this PR does

Allows the Istio control plane (istiod) to configure ztunnel's TLS parameters
— minimum/maximum protocol version, cipher suites, and ECDH key exchange groups
— at runtime via the XDS Delta ADS stream, without requiring a ztunnel restart.

A new XDS resource type, `MeshTLSConfig` (defined in `workload.proto`), carries
the global TLS profile for the mesh. ztunnel subscribes to this type alongside
the existing `Address` and `Authorization` types. When a profile is received,
new TLS connections use it; when no profile is received, existing compiled-in
defaults apply unchanged.

A companion PR to `istio/istio` (link TBD) adds the istiod-side generator that
produces `MeshTLSConfig` resources from `meshConfig.tlsDefaults`.

---

## Changes

### `proto/workload.proto`
- Add `MeshTLSConfig` message with fields: `min_protocol_version`,
  `max_protocol_version`, `cipher_suites`, `ecdh_curves`.
- Add `TLSProtocolVersion` enum (`TLS_AUTO`, `TLSv1_2`, `TLSv1_3`).

### `src/xds/types.rs`
- Add `MESH_TLS_CONFIG_TYPE` constant:
  `"type.googleapis.com/istio.workload.MeshTLSConfig"`.

### `src/tls/config.rs` (new file)
- `TLSProfile` — internal representation of a received TLS profile.
- `TLSProfileStore` — `Arc<RwLock<Option<TLSProfile>>>` shared between the XDS
  handler (writer) and the TLS configuration functions (readers).

### `src/state.rs`
- `MeshTLSConfigHandler` implementing `Handler<XdsMeshTLSConfig>`:
  - `no_on_demand() → true`: this is a singleton resource, not on-demand.
  - On update: parse and validate the proto; write to `TLSProfileStore`; NACK if
    the configuration is invalid (e.g. `min_version > max_version` or the
    resulting cipher list would be empty).
  - On remove: clear the stored profile, falling back to compiled-in defaults.
- Register `MeshTLSConfigHandler` with `.with_watched_handler()` in
  `ProxyStateManager::new()`.

### `src/tls/lib.rs`
- Add `tls_versions_with_profile(Option<&TLSProfile>)` — returns the rustls
  protocol version slice corresponding to the profile; falls back to
  `tls_versions()` when `None`.
- Add `provider_with_profile(Option<&TLSProfile>)` — returns a `CryptoProvider`
  with cipher suites and kx groups filtered to those permitted by the profile;
  falls back to `provider()` when `None`. Filtering is always a subset of the
  compiled-in provider to guarantee no unsupported suite is ever selected.

### `src/tls/certificate.rs`
- `server_config()` and `client_config()` accept an additional
  `Option<&TLSProfile>` parameter and pass it to the new `lib.rs` functions.
- `outbound_connector()` accepts `Option<&TLSProfile>` and threads it through.

### `src/proxy/inbound.rs`
- `InboundCertProvider` holds a `TLSProfileStore` reference and reads the
  current profile when `fetch_cert()` is called.

### `src/proxy/pool.rs`
- `new_pool_conn()` reads the current profile from `TLSProfileStore` and passes
  it to `outbound_connector()`.

---

## Backwards compatibility

- Deployments without a `MeshTLSConfig` resource from the control plane behave
  identically to the current behaviour.
- `TLS12_ENABLED` and `COMPLIANCE_POLICY` environment variables remain
  functional as the default when no XDS profile is active.
- Only **new** connections use the updated profile; in-flight TLS sessions are
  not renegotiated (this is standard TLS behaviour).

---

## Testing

- **Unit — handler ACK/NACK**: valid profile is stored; invalid profile
  (`min > max`, empty cipher list after filtering) produces a `RejectedConfig`
  and triggers an XDS NACK.
- **Unit — `tls_versions_with_profile`**: correct version slice for each profile
  variant; `None` returns the same result as the unmodified `tls_versions()`.
- **Unit — `provider_with_profile`**: cipher suite list is a subset of the
  base provider; empty input → base provider unchanged.
- **Unit — remove**: after `XdsUpdate::Remove`, `TLSProfileStore` returns
  `None` and connections revert to defaults.
- **Integration**: mock XDS server pushes `MeshTLSConfig{min=TLS13}`; verify
  that a TLS 1.2 client connection is rejected. Push `MeshTLSConfig{min=TLS12}`;
  verify that TLS 1.2 client connection succeeds.
