# ADR-0051: Remove the non-Java unregister request body

- Status: Accepted
- Date: 2026-10-09

## Context

The Rust protocol crate exposed `UnregisterClientRequestBody` as a public DTO and tested its JSON serialization, and also had an unused `UnregisterClientBody` public type. Pinned RocketMQ 5.5.1 client, common, and remoting artifacts contain neither class. `MQClientAPIImpl.unregisterClient` creates request code 35 (`UNREGISTER_CLIENT`) with an `UnregisterClientRequestHeader` and sends the request without a body.

## Decision

Remove both Rust-only body DTOs. Keep the existing `UnregisterClientRequestHeader` DTO and its Java-shaped fields/accessors as the wire representation. Registry operations named `unregister_consumer` and `unregister_producer` remain lifecycle operations; they are distinct from the remoting API request.

## Consequences

The public Rust protocol surface no longer advertises an unregister body that Java does not define. This correction does not alter command encoding or header behavior. Phase 6 remains PARTIAL, and P6-V04 stays `PENDING_PHASE_19`.

## Evidence

- `jar tf` over pinned `rocketmq-client-5.5.1.jar`, `rocketmq-common-5.5.1.jar`, and `rocketmq-remoting-5.5.1.jar` found neither `UnregisterClientRequestBody.class` nor `UnregisterClientBody.class`.
- `javap -c` for pinned `MQClientAPIImpl.unregisterClient` shows construction of `UnregisterClientRequestHeader`, request code 35, and invocation without a body. `javap` on the header confirms its no-argument constructor and `clientID`, `producerGroup`, and `consumerGroup` accessors.
- Implementations: `0c60e2b1` (request-body DTO/test), `a1f8d747` (orphan body DTO).
- Validation: `cargo test --workspace --all-targets --locked`; `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`; `cargo fmt --all --check`; `git diff --check`; repository search for remaining code references.
