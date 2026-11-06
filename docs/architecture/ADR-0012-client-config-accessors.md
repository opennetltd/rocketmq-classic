# ADR-0012: Map Java ClientConfig accessors to Rust methods

Status: implemented for all 33 non-internal settings; broader Phase 6 source and input reconciliation remains open.

## Source contract

Pinned Java 5.5.1 `ClientConfig` exposes getter/setter pairs for every public setting. The plain accessors read or assign the stored value without validation. String settings accept null; primitive settings preserve their Java signed widths. `namesrvAddr` and `namespace` also affect the private namespace-cache state, so their existing Rust methods retain those transitions.

## Rust representation

Each setting now has an idiomatic Rust getter and `set_` method. Nullable strings use `Option<String>` setters and `Option<&str>` getters; language and access-channel getters return nullable copied enums; primitive getters/setters preserve `i32`, `i64`, and `bool`. Existing public fields remain available for aggregate configuration use, and no new settings are added. Namespace and NameServer continue through their cache-aware methods.

## Evidence

The Java `copyFields` oracle invokes every Java setter, snapshots source/clone/reset fields, and now directly invokes every Java getter for the same three objects. The native differential constructs the source through Rust setters, reads through Rust getters, and matches both Java getter output and field snapshots for all 33 settings. This includes explicit null strings, nullable enums, and signed integer boundaries. Client-config fixture SHA-256 at this accessor update: `2824c98496001ab46cc7137400359f51b2afe443d6fee779946606d78f379463`.
