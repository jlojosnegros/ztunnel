# Feature request: dynamic TLS profile configuration via XDS

## Summary

ztunnel's TLS parameters (minimum protocol version, cipher suites, and key
exchange groups) are determined at compile time or at process startup through
environment variables. There is currently no mechanism for the control plane
to update these settings after ztunnel has started, without redeploying or
restarting every node-level ztunnel instance.

This makes it impossible to enforce organization-wide TLS policies across an
Ambient mesh without coordinating ztunnel restarts, and prevents organizations
with strict compliance requirements from adopting Ambient mode at all.

---

## Problem

### 1. Static TLS configuration breaks FIPS compliance for Ambient mode

This issue is a direct blocker for Ambient mode in FIPS-constrained environments,
as documented in [istio/istio#52926](https://github.com/istio/istio/issues/52926).

When `COMPLIANCE_POLICY=fips-140-2` is set in Istio, istiod configures Envoy
sidecars to accept TLS 1.2 only. However, ztunnel's default configuration
enforces TLS 1.3 as the minimum version. This creates an incompatibility:
**Envoy proxies in FIPS mode cannot establish HBONE connections with ztunnel**,
because their maximum TLS version (1.2) is lower than ztunnel's minimum (1.3).

The same issue applies in reverse: an organization may want to enforce TLS 1.3
across their mesh, but ztunnel's `TLS12_ENABLED=true` flag is a per-deployment
setting that cannot be toggled centrally without restarting every ztunnel pod
cluster-wide.

### 2. No central control over ztunnel cipher suites

istiod already supports propagating TLS defaults to Envoy proxies via
`meshConfig.tlsDefaults` and `meshConfig.meshMTLS`. These settings allow
operators to restrict or expand the set of accepted cipher suites for
sidecar-mode proxies. **ztunnel does not participate in this mechanism at all.**

ztunnel's cipher suites are hardcoded per crypto provider
(`src/tls/lib.rs`, lines 76–161), controlled only by two static environment
variables (`TLS12_ENABLED`, `COMPLIANCE_POLICY`) read once at startup. There is
no way to update them without redeploying every ztunnel instance in the cluster.

### 3. Ambient mode cannot be managed by the same TLS policies as sidecar mode

Organizations operating mixed meshes (some namespaces in sidecar mode, others
in Ambient mode) face an asymmetry: they can centrally manage TLS configuration
for Envoy proxies via MeshConfig, but must manage ztunnel's TLS configuration
out-of-band, using environment variables and DaemonSet restarts. This creates an
operational burden and is an inconsistency in the control plane API.

---

## Current behaviour

ztunnel reads TLS settings from two static `Lazy<bool>` variables initialized
at startup (`src/lib.rs`):

```rust
// Compile-time / startup-time only; never updated at runtime
static TLS12_ENABLED: Lazy<bool> =
    Lazy::new(|| env::var("TLS12_ENABLED").unwrap_or_default() == "true");

static PQC_ENABLED: Lazy<bool> =
    Lazy::new(|| env::var("COMPLIANCE_POLICY").unwrap_or_default() == "pqc");
```

These values are consumed by `tls_versions()` and `provider()` in
`src/tls/lib.rs` and flow into every `ServerConfig` and `ClientConfig`
constructed by `server_config()` and `client_config()` in
`src/tls/certificate.rs`. Because they are `Lazy` statics, they can only change
with a process restart.

ztunnel subscribes to two XDS resource types from istiod
(`src/xds/types.rs:44-46`):

- `type.googleapis.com/istio.workload.Address` — workload and service data
- `type.googleapis.com/istio.security.Authorization` — RBAC policies

Neither carries global mesh configuration such as TLS defaults.

---

## Proposed solution

Add a new XDS resource type, `MeshTLSConfig`, that istiod pushes to ztunnel
instances. ztunnel subscribes to this resource type and applies its contents
dynamically when establishing new connections.

### Proto definition (in `workload.proto`)

```protobuf
// MeshTLSConfig carries the global TLS configuration for the mesh.
// It is pushed by the control plane to configure TLS parameters on
// ztunnel instances, overriding compiled-in defaults.
// The primary XDS key for this resource is the mesh name (e.g. "default").
message MeshTLSConfig {
  // min_protocol_version is the minimum TLS version to accept.
  // If TLS_AUTO, the compiled-in default is used.
  TLSProtocolVersion min_protocol_version = 1;

  // max_protocol_version is the maximum TLS version to negotiate.
  // If TLS_AUTO, the compiled-in default is used.
  TLSProtocolVersion max_protocol_version = 2;

  // cipher_suites is the set of cipher suites to allow.
  // Applies to TLS 1.2 connections only; TLS 1.3 suites are not
  // negotiable per RFC 8446 and are always included when TLS 1.3 is enabled.
  // If empty, the compiled-in defaults are used.
  repeated string cipher_suites = 3;

  // ecdh_curves lists the ECDH groups for key exchange.
  // If empty, the compiled-in defaults are used.
  repeated string ecdh_curves = 4;
}

enum TLSProtocolVersion {
  TLS_AUTO = 0; // Use the compiled-in default
  TLSv1_2  = 1;
  TLSv1_3  = 2;
}
```

### XDS subscription (ztunnel side)

A new type URL constant is added:

```rust
// src/xds/types.rs
pub const MESH_TLS_CONFIG_TYPE: Strng =
    strng::literal!("type.googleapis.com/istio.workload.MeshTLSConfig");
```

A `MeshTLSConfigHandler` implementing the existing `Handler<T>` trait processes
incoming updates and stores the active profile in a shared, thread-safe
`TLSProfileStore`. The handler is registered alongside the existing Address and
Authorization handlers:

```rust
// src/state.rs
xds::Config::new(config.clone(), tls_client_fetcher)
    .with_watched_handler::<XdsAddress>(xds::ADDRESS_TYPE, updater.clone())
    .with_watched_handler::<XdsAuthorization>(xds::AUTHORIZATION_TYPE, updater)
    .with_watched_handler::<XdsMeshTLSConfig>(          // new
        xds::MESH_TLS_CONFIG_TYPE,
        MeshTLSConfigHandler::new(tls_profile.clone()),
    )
```

`tls_versions()` and `provider()` in `src/tls/lib.rs` are extended to accept an
`Option<&TLSProfile>` and fall back to their current compiled-in defaults when
no profile has been received from the control plane, preserving full backwards
compatibility.

### Control plane side (istiod)

A complementary change in istiod adds a `MeshTLSConfig` XDS generator that
reads from `meshConfig.tlsDefaults` / `meshConfig.meshMTLS` and pushes the
resulting resource to connected ztunnel instances. This will be submitted as a
separate PR to the `istio/istio` repository.

### Why a new XDS resource type, not PCDS or Workload extensions?

| Option                                    | Why not                                                                                                                              |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Extend PCDS (`ProxyConfig`)               | PCDS is gated on `features.MultiRootMesh`; `ProxyConfig` is an Envoy-specific proto not appropriate for ztunnel mesh-level config    |
| `Extension` field on `Workload`/`Service` | TLS config is global, not per-workload; embedding it in every workload resource is semantically wrong and wastes bandwidth           |
| Environment variables                     | Cannot be updated without a DaemonSet rollout; this is precisely the problem we are solving                                          |
| **New XDS resource type**                 | Follows the established pattern (`Address`, `Authorization`); semantically correct; independently versioned; zero-cost when not used |

---

## Backwards compatibility

- If no `MeshTLSConfig` resource is received (e.g., older istiod), ztunnel
  behaves exactly as today.
- If a `MeshTLSConfig` with all-default fields is received, ztunnel also
  behaves exactly as today.
- TLS configuration only affects **new** connections. Existing mTLS sessions
  are not renegotiated; this is correct TLS behaviour and is consistent with
  how Envoy handles configuration changes.
- The existing `TLS12_ENABLED` and `COMPLIANCE_POLICY` environment variables
  continue to work as the fallback/default when no XDS profile is active,
  maintaining full backwards compatibility for deployments that do not use the
  new feature.

---

## Impact and motivation

- Unblocks FIPS-compliant Ambient mode deployments
  ([istio/istio#52926](https://github.com/istio/istio/issues/52926)).
- Brings ztunnel's TLS policy management in line with sidecar proxies
  (`meshConfig.tlsDefaults`), providing a consistent control plane API for
  both data plane modes.
- Allows operators to enforce TLS version and cipher policies centrally,
  without coordinating DaemonSet restarts.
- Closes [ztunnel#21](https://github.com/istio/ztunnel/issues/21) ("Tune TLS
  settings") by providing a principled, control-plane-driven approach.

---

## Proposed work breakdown

1. **`workload.proto`** — add `MeshTLSConfig` message and `TLSProtocolVersion` enum
2. **`src/xds/types.rs`** — add `MESH_TLS_CONFIG_TYPE` constant
3. **`src/tls/config.rs`** (new) — `TLSProfile` struct and `TLSProfileStore`
4. **`src/state.rs`** — `MeshTLSConfigHandler` and handler registration
5. **`src/tls/lib.rs`** — `tls_versions_with_profile()`, `provider_with_profile()`
6. **`src/tls/certificate.rs`** — thread `TLSProfile` through `server_config()` and `client_config()`
7. **Tests** — unit tests for handler ACK/NACK, profile parsing, and TLS version selection; integration test verifying that connections respect the XDS-provided profile
8. **istiod** — companion PR in `istio/istio` to add the `MeshTLSConfig` generator

Happy to take this on. Feedback on the design before I write code is very welcome.
