# ADR-0007: Nullable command language

Status: implemented in `63b7feb` under the user's explicit public API parity instruction. Phase 6 as a whole remains incomplete.

## Evidence

Pinned client 5.5.1 ClientConfig.setLanguage accepts null; clone/reset retain it,
and MQClientInstance construction succeeds. An actual unstarted runtime and
RemotingCommand.encode invocation in RuntimeConfigFixtures prove:

- JSON encoding succeeds, omits the language key, and decoding the omitted key
  produces Java's default JAVA language.
- ROCKETMQ binary encoding throws NullPointerException for null language.

The prior native ClientConfig and RemotingCommand required LanguageCode and
always emitted a JSON language. The source-backed red test `40bd7cf`
demonstrated that mismatch before the model migration. An additional actual
Java decode case proves explicit `"language":null` decodes to null, while an
omitted field decodes to default JAVA.

## Proposed decision

Use Option<LanguageCode> for ClientConfig.language and RemotingCommand.language.
Preserve existing defaults as Some(Java) and Some(Rust), respectively. Core typed
RPCs keep Java-default language except heartbeat, which uses the nullable client
setting. JSON omits None, decodes an absent key to Some(Java), and must distinguish
an explicitly supplied null according to the Java decoder. Binary encoding of
None returns a native codec error, never a fabricated language byte.

Update hooks, request/response models, codecs and all construction/tests together.
Before/after hooks must observe the actual nullable field, allowing an owner hook
to replace it as Java permits; a core-only early rejection or hidden sentinel
would alter that control flow. Existing valid/unknown language behavior remains
subject to its source-backed lower-layer contract.

## Review boundary

RemotingCommand is a completed Phase 2 public model. The execution index requires
review and an ADR before changing it. The owner-approved earlier heartbeat Vec
change does not automatically approve this unrelated lower public field change.
The active user objective explicitly requires public API and setting parity,
which authorized this correction. It does not imply Phase 6 acceptance or a
waiver of other public-model/input gaps. Independent nullable AccessChannel,
SOCKS and Broker-address corrections have their own evidence.

## Implementation evidence

`ClientConfig.language` and `RemotingCommand.language` are `Option<LanguageCode>`;
defaults remain `Some(Java)` and `Some(Rust)`. JSON omits None and distinguishes
absent from explicit null on decode. Binary encode returns `MissingLanguage`
for None. Binary decode now uses Java's `LanguageCode.valueOf(byte)`, returning
None for unknown codes; JSON string decode uses `getCode(String)` and likewise
returns None for unknown names. Core heartbeat V1/V2 use the nullable config;
other typed RPC requests continue to use Java. See [ADR-0023](ADR-0023-language-code-java-methods.md)
for the pinned enum method oracle and fixture hash recorded at that LanguageCode update. The config fixture
SHA-256 at this ADR's update was
`9b7a324ef971a1ea4dc3642dd54f1a3bd432684c8ef5dac03de89a827a52d6f4`.
Native config clone/reset/runtime, codec JSON/binary and scripted hook/wire tests
pass. Full Rust workspace, exact all-target/all-feature Clippy, fmt/diff,
comparator/manifest and pinned offline Maven verify exited 0.
