# 第三輪相容性 Review：LZ4 stream 解碼與 live trace 身分

## 1. 基準、範圍與結果

日期：2026-10-27。Review revision：`cbc1bae1fa98e2ef19a33580c3852b77b6ceb1bc`。最新 production 修復為 `92940332`、`79cd6caf`、`9d944e78`、`08255f32`；`cbc1bae1` 記錄驗證結果。兩個既有未追蹤 review-evidence 目錄均保留。

核心原則維持使用者要求：公開 API 能力、輸入範圍與設定對應 Java RocketMQ client 5.5.1，執行流程一致，Rust 可選擇自己的元件。不要求模擬 Java／Netty／JVM 元件。本機固定 client source JAR SHA-256 為 `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`；Java LZ4 相依版本為 `org.lz4:lz4-java:1.8.0`。

**確認兩項新問題：一項 P1、一項 P2。** 兩者均為先前未覆蓋的相鄰路徑，並非上一輪五個具體修復被撤回。新問題由本文件追蹤，前次文件保留為有日期的歷史紀錄。本次只新增文件與 probe artifacts。

| ID | 優先級 | 問題 | 證據 |
| --- | --- | --- | --- |
| G01 | P1 | 合法 concatenated／skippable LZ4 stream 被拒絕；合法 Pull record 變成空的 FOUND 結果 | 相同 bytes 實際通過 Java／Rust compressor 與 PullAPIWrapper |
| G02 | P2 | 公開 live group setter 改值後，Lite consume trace 仍使用 startup group | Rust 實際 outbound trace SEND row，對照固定 Java poll／trace-hook append 呼叫 |

P1 表示 client 邊界已重現漏交付，offset 後果另以來源追查；P2 表示可觀察的 trace 歸屬錯誤。兩者均不依賴 executor 結構或偶發 timing 完全相同。

範圍：完整最新修復 diff，以及相鄰 Common compression／message decoding、Consumer Pull／Lite／trace、Producer compressor／trace publication 與其 runtime／RPC 邊界。本輪未完整重審所有未變動 Protocol／Remoting 實作。

## 2. 前次修復與執行驗證

| 前次問題 | 本 revision 的驗證 |
| --- | --- |
| F01，nullable Lite poll 遺失 batch | 更新後的原 probe 在 null group 下回傳一筆訊息，交付後持久化 offset 1；hook regression 也通過。 |
| F02，inner decoder 限制／繼承狀態 | 更新後的原 probe 回傳 24 筆大型 inner message，小型 inner message 繼承 WAIT=false；Consumer 已使用 Common decoder。Large／mixed／subtype／fallback regressions 通過。 |
| F03，orderly wait-ACK 釋放 | 已移除 decrement；既有 regression 在 threshold 96 下保留 count 97 並通過。原 Java probe 也保留其一筆 outstanding message。 |
| F04，公開解壓上限 | 更新後的原 probe 成功 round-trip 65 MiB；Common 各 codec 與 Producer snapshot／large-body regressions 通過。G01 是 stream 結構問題，不是已移除的大小上限。 |
| F05，startup namespace mutation | 更新後的原 probe 在失敗／成功 startup 後看到 wrapped group；lifecycle tests 通過。G02 是之後的 trace 歸屬問題。 |

`cargo test --workspace --locked` exit 0：94 筆 test-result 摘要，共 **2,059 passed、0 failed、0 ignored**，包含 doctests。[前次案例重跑結果](04-project-parity-third-review-evidence/previous-review-check.json) 記錄五個已修正邊界；[新 probe 結果](04-project-parity-third-review-evidence/results.json) 保留 source／probe hash、revision、輸出與執行限制。

備妥 Maven／Rust 相依快取、JDK、Maven 與 repository toolchain 後，可重現新問題：

```sh
python3 plan/2026/10/04-project-parity-third-review-evidence/run_probes.py --output /tmp/rocketmq-third-review-results.json
```

Probe 建立連結目前 production library 的暫存 crate，重用既有 scripted Lite TCP helpers 而不修改它們。Rust trace row 取自送往 loopback peer 的實際 outbound SEND command。Java 直接呼叫固定 compressor／PullAPIWrapper；Lite poll 則注入 running／cache state，擷取實際 trace-hook append 呼叫。未連線 live RocketMQ Broker／NameServer。腳本 assertion 刻意描述目前缺陷，轉成正確行為 regression 時須更新。本輪未重跑完整 Maven fixture verify、Clippy、Docker／live scenarios 或跨平台檢查。

## 3. 問題與改善方向

### 3.1 G01 — LZ4 stream 只支援一個 ordinary frame

**位置：** [LZ4 parser](../../../crates/rocketmq-classic-common/src/compression.rs#L100)、[全輸入結束檢查](../../../crates/rocketmq-classic-common/src/compression.rs#L232)、[compatible batch decoder](../../../crates/rocketmq-classic-common/src/message/decoder.rs#L585)、[Pull processing](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L162)。Java source：Common `org/apache/rocketmq/common/compression/Lz4Compressor.java`；相依來源為 1.8.0 的 `net/jpountz/lz4/LZ4FrameInputStream.java`。

Rust 只解析一個 LZ4 frame，拒絕非 ordinary 的起始 magic，並在該 frame 後要求 `cursor == frame.len()`。Java RocketMQ compressor 使用 `LZ4FrameInputStream` 預設的 multiple-frame mode，讀取至結尾，包含 skippable frame。移除解壓大小上限，未修正此輸入範圍差異。

**實際執行案例：**

| 輸入 | Java 結果 | Rust 結果 |
| --- | --- | --- |
| 兩個各自合法的 LZ4 frame，內容為 `first-`、`second` | `first-second` | InvalidData：`invalid LZ4 frame` |
| 一個帶三個 payload bytes 的合法 skippable frame，後接上述 frames | `first-second` | InvalidData：`invalid LZ4 frame` |
| 將串接 bytes 作為一筆其餘欄位合法 MessageExt record 的壓縮 body，next offset 1 | FOUND、一筆訊息、body `first-second`、next offset 1 | FOUND、零筆訊息、next offset 1 |

Record 設定 `COMPRESSED_FLAG` 與 LZ4 compression flag，編碼時不再壓縮已編碼 stream。兩個 wrapper 收到相同 bytes。不宣稱 Java 預設 producer 會產生串接 frames，也不宣稱 live 5.3.4 Broker 產生過此案例。

**影響：** 公開解壓拒絕 Java 可讀的合法 stream。Consumer processing 把該拒絕轉成 decoder 的 prefix／stop 行為，將合法 record 隱藏為空的成功 pull。Push／Lite caller 保留 Broker 的 next offset，因此與其他把合法 record 誤判為 malformed 的情況一樣有漏交付風險。空 FOUND 邊界已實際執行；durable Broker offset 推進採來源追查，未連線 live Broker 驗證。

**實作：** 將 LZ4 stream 當成 ordinary／skippable frame 序列解析，對應固定 Java 相依接受的 framing。串接 ordinary frame 輸出，安全略過 skippable 的宣告 payload。各 frame 邊界檢查 header、length、content size、checksum。明確的內部 output budget 須跨整個 stream 累計，不可每個 frame 重新獲得額度。Rust 可使用自己的 parser／library；可觀察的接受範圍與結果須對應 Java。真正 malformed-stream 行為與合法 multi-frame 處理分開驗證。

**驗收：** 補固定 Java／公開 Rust 比較：單 frame、雙 frame、frame 前／中間的 skippable、空 data frame、截斷 header／skip payload、錯誤 checksum、無效後續 frame。Malformed 預期先與 Java 比較再決定。合法 compressed-record 須回傳一筆邏輯訊息與完整 body；新增 Push／Lite offset regression，避免合法 stream 以空成功消失。保留前次 65 MiB 與明確 internal-bound tests。

### 3.2 G02 — Lite consume trace 忽略 live group／context

**位置：** [startup hook 建立](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L812)、[captured group field](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L116)、[after-hook 使用 captured group](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L143)、[poll context normalization](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1134)。Java source：client `impl/consumer/DefaultLitePullConsumerImpl.java:606`、`trace/hook/ConsumeMessageTraceHookImpl.java:55,100`。

`LiteConsumeTraceHook` 保留 startup `consumer_group`，並用它建立兩筆 trace row。Java before hook 從目前 `ConsumeMessageContext` 取得 group、去除 namespace 後輸出 trace；after hook 沿用該 before-trace state。因此後續支援的 group mutation 會影響 Java trace 歸屬，但不影響 Rust。

**實際重現：** 啟用 Lite tracing，以 namespace `tenant`、group `initial-group` 啟動，之後呼叫 `set_consumer_group("later-group")` 並 poll 一筆有 region 的訊息。Rust 正常交付訊息，但 outbound trace SEND 的 `SubBefore`、`SubAfter` 均為 group `initial-group`。固定 Java poll／hooks 的兩筆 row 都是 `later-group`，consume context 也是 `later-group`。Probe 驗證 trace 內容，不表示 Broker registration／subscription 已轉移到新 group。

相關 poll code 建立 hook context 前，也會對後續 non-null group 再呼叫 `with_namespace`；Java 直接透過 `groupName()` 傳遞目前 facade field。此 context 重包 namespace 為來源確認，外部 probe 未另外觀察 Rust custom-hook context 值；trace 差異本身則直接取自 wire。

**實作：** Consume-trace identity 取自實際 before-hook context，保留 Java 的 nullable group 與 namespace-removal 行為。對應的 after row 沿用 before-trace identity，不在 before 階段後重新讀取已變動的 group。Per-poll tracing 不再以 startup-group snapshot 作決策。Lite hook context 依 Java setter／start mutation 邊界傳遞目前 facade group；runtime registration ownership 維持其獨立已審查的 capture 邊界。

**驗收：** 比較 startup group、後續 plain group、後續 namespace-qualified group、null group 的 before／after trace group 與 hook context，分別有／無設定 namespace。同一次 consume event 的兩筆 row 須保留 Java before-stage identity。維持 message-ID、topic／namespace、region、TRACE_ON filtering、success、access-channel、timing 語意。Nullable-poll delivery／commit test 繼續通過，trace 變更不得移動 consumer registration 或 offset-store ownership。

## 4. 實作順序與驗證

1. 修正 G01 framing，補公開 decompressor 與完整 record pull regressions；private bounded path 增加跨 frame 累計預算測試。
2. 修正 G02 per-consume trace identity 與 hook-context 來源，將實際 outbound trace row 與既有 live-group／null-poll lifecycle 一起驗證。
3. Focused Common／Consumer／Producer checks 與固定 Java invocation cases 通過後，執行既有 gates：

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

目前 2,059 項通過仍是 non-regression 基準。保留前次修復與 malformed-input contract 覆蓋；在可呼叫／輸出邊界驗證接受的 stream shape 與 dynamic identity，不只依賴自身 compressor round-trip 或 startup-state assertion。

環境相依驗證放最後：有對應情境時，重跑受影響的既有 local Pull／Lite／trace scenarios，保留連結 revision 的證據。Baseline Broker 不產生的合法 stream shape，以 scripted peer 驗證。這些問題不需要 owner 正式 credentials／基礎設施；本次 review 不變更 CI 政策，也不代表已可發布。
