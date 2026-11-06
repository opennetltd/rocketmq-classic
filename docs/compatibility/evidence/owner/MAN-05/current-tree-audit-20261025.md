# MAN-05 source-backed feature audit and disposition

Date: 2026-10-25

Disposition: N/A for additional feature-specific owner validation. This does
not approve production use or close the separately listed MAN-01 through
MAN-04 owner-environment checks.

## Scope and pinned source

- Java oracle: Apache RocketMQ 5.5.1, source commit
  `efa1be59d6fc747ad220f036c86030f05f0419f9`.
- P18 final API/source audit:
  [PHASE_6_REQUIREMENT_AUDIT.md](../../../PHASE_6_REQUIREMENT_AUDIT.md), including
  132 MQClientAPIImpl declarations, seven overload groups, 29 direct RPC
  invocation cases, Rust reverse exports, configuration/settings, inputs,
  errors, and control flow.
- Remoting setting and public-surface inventory:
  [PHASE_3_REMOTING_INVENTORY.md](../../../PHASE_3_REMOTING_INVENTORY.md).
- P18 final closure code revision: `0cf294b710d06d40bd72ccec1ca5d1df121c5225`.
- Current production code revision under audit:
  `d3c7d2e845364e40ad72f0540a0a498fc1582247`. Two follow-up code commits
  remove redundant borrows found by strict Clippy; they do not change runtime
  behavior. Other later commits updated evidence pointers and documentation.

## Post-closure source/API delta

The production-source review from the P18 closure through the current code
revision found PushConsumer flow corrections, TLS verifier behavior, and
lifecycle counters used only by the differential test harness. It found no
additional production public API, configuration setting, or client feature
without a Java 5.5.1 counterpart.

1. **Push subscription and pull flow now follows Java 5.5.1.** Java's
   `DefaultMQPushConsumerImpl.subscribe` stores subscription data and attempts
   a heartbeat; `unsubscribe` removes the table entry. Neither method invokes
   an immediate rebalance. `pullMessage` captures the `SubscriptionData` used
   for the request and passes that same snapshot to `processPullResult`. On
   `SUBSCRIPTION_NOT_LATEST`, the pull callback logs the event and schedules
   the normal delayed retry (with a distinct delay only for `FLOW_CONTROL`);
   heartbeat work remains independent. Rust follows those paths: subscription
   heartbeat transport failures remain best effort, unsubscribe does not
   trigger rebalance, code 25 does not send an extra heartbeat, and results use
   the subscription snapshot captured for the pull. The clean live
   P6-V04 run (raw output not retained) verifies subscription
   updates, tag filtering, and the remaining shared producer. Pinned source:
   [DefaultMQPushConsumerImpl.java](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.1/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java#L335-L350),
   [subscription methods](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.1/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java#L1265-L1309),
   and [stale-subscription retry](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.1/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java#L435-L449).

2. **Lifecycle counters are not part of the normal Rust API.**
   `RemotingLifecycleCounts` and `RemotingClient::lifecycle_counts()` are
   available only when the differential harness enables
   `rocketmq_classic_test_support`. Normal crate builds do not export them.
   The harness uses them to assert zero pending requests, channels, and owned
   tasks at shutdown; they add no Java-facing capability or setting.

3. **Verified TLS matches Java's endpoint-identification behavior.** Java
   `TlsHelper.buildSslContext(true)` enables CA trust validation when
   `tls.client.authServer=true`; `NettyRemotingClient` creates the handler with
   `sslContext.newHandler(ch.alloc())`, without a peer hostname. Rust verified
   mode now checks the configured trust chain and certificate validity but
   does not compare the requested hostname. It still rejects untrusted and
   expired certificates and still verifies handshake signatures. Fresh
   P3-V04 evidence (raw output not retained) confirms that
   both clients accept the CA-trusted wrong-host certificate and reject an
   untrusted CA. Fresh P16-V02 evidence (raw output not retained)
   confirms the same behavior under mutual TLS while rejecting missing client
   certificates. The TLS-terminating proxy forwards to the isolated plain
   Broker; these cases validate client TLS behavior, not Broker TLS
   configuration.

The source/API rows above close the identified post-P18 code deltas. The
complete P18 audit remains the record for the broader scoped API, overload,
setting, input, error, control-flow, and reverse-export review. The historical
`run-local.sh --all` replay passed all 23 gates at workspace revision
`c877869bd1f9bfacf288a27593649cca12ed9c5d`, which contains production code
revision `d3c7d2e845364e40ad72f0540a0a498fc1582247`. Its final reconciliation
verified all 37 manifest rows passed from clean source with exit status 0.
The repository now retains only their `PASS_RECORDED` manifest summaries and
not the raw run outputs. Corrected live outcomes were recorded for
P3-V04 (raw output not retained),
P6-V04 (raw output not retained), and
P16-V02 (raw output not retained). The run evidence and its manifest pointers
were removed from the repository during test-artifact cleanup.

## MAN-05 decision

No additional feature-specific validation requires an owner-only service or
topology. Owner-specific checks remain separate: MAN-01 covers network/DNS/VIP
paths, MAN-02 organization security material and ACL policy, MAN-03 authorized
HA fault topology, and MAN-04 representative workload and SLO capacity. Their
absence does not leave an unclassified client feature. No owner or production
environment was accessed for this audit.
