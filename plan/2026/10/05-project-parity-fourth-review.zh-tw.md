# 第四輪一致性 review：trace namespace 語義

## 1. 基準、範圍與結果

日期：2026-10-27。Review revision：`bee48c30a59660accfdb05676756b8d8668408aa`。本輪檢查第三輪修復的完整差異，以及相鄰的壓縮、訊息解碼、Pull/Lite 投遞、consume hook 與 Consumer/Producer trace 流程。未重新窮盡稽核未變更的 Protocol/Remoting 行為。

沿用使用者原則：公開 API／輸入／設定與 Java RocketMQ client 5.5.1 一致，執行流程邏輯一致，允許採用 Rust 原生組件。固定版本 client source JAR SHA-256 為 `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`。

**確認 1 項新的 P2 問題：H01，trace namespace 正規化與 Java 不一致。** 影響含 `%` 合法名稱的 trace 歸屬；未確認新的投遞或 offset 缺陷。先前文件保留為歷史紀錄。本輪僅新增文件與 probe 證據。

## 2. 前輪修復驗證與執行證據

| 檢查 | 此 revision 的結果 |
| --- | --- |
| G01，LZ4 stream | 重播公開解壓縮與 Pull probe：單一／串接／skippable／空 frame 成功；損壞的 header、skip payload、checksum 與後續 frame 案例在 Java、Rust 都失敗。Pull 投遞 1 筆 `first-second`，next offset 為 1。 |
| G01，投遞／commit 回歸 | 新增的 scripted Lite 測試通過，先投遞串接 frame 訊息再持久化 offset 1。原始碼確認輸出上限為累計計算，並逐 frame 檢查。 |
| G02，即時 group | 前輪 probe 的 Rust outbound trace 兩列現在皆為 `later-group`，與 Java 一致。即時 facade group 與 nullable／啟動 hook 測試通過。H01 涉及先前未涵蓋的 namespace 輸入形狀。 |
| Workspace | `cargo test --workspace --offline` exit 0：含 doctest 共 94 個結果摘要，**2,067 passed、0 failed、0 ignored**。 |

[前輪案例重播](05-project-parity-fourth-review-evidence/previous-review-check.json)、[新差異測試結果](05-project-parity-fourth-review-evidence/results.json) 與[測試摘要](05-project-parity-fourth-review-evidence/workspace-test-summary.json) 保留 revision、source/probe hash、結果與執行邊界。既有證據目錄均保留。

使用已快取的依賴、JDK、Maven 與專案 toolchain 重現 H01：

```sh
python3 plan/2026/10/05-project-parity-fourth-review-evidence/run_probes.py --output /tmp/rocketmq-fourth-review-results.json
```

Rust 使用目前 production library 與 scripted loopback NameServer/Broker peer，probe 讀取實際 outbound trace SEND 列。Java 透過 reflection 設定 running/cache 狀態，呼叫實際 Lite poll/trace hook，且 `Validators.checkGroup` 接受每個測試 group。每次 poll 均投遞 1 筆訊息。未連線至真實 RocketMQ 服務。本輪未重跑完整 Maven fixture 驗證、Clippy、Docker/live 情境與跨平台檢查。

## 3. H01 — Trace namespace 移除使用了錯誤的 overload 與階段

**優先級：P2。** 錯誤的 trace group/topic 歸屬，會使搜尋與監控分散到 Java 正規化結果以外的名稱。

**Rust 位置：**Lite 的 [before 快照](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L138)、[context 建構](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L187) 與 [namespace helper](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L264)；一般 Consumer 的 [trace helper](../../../crates/rocketmq-classic-consumer/src/consume/trace.rs#L146)；Producer 的 [trace helper](../../../crates/rocketmq-classic-producer/src/trace.rs#L363)。既有 [NamespaceUtil](../../../crates/rocketmq-classic-protocol/src/namespace.rs#L33) 已提供兩種對應 Java 的操作。

**固定版本 Java 原始碼：**client `trace/hook/ConsumeMessageTraceHookImpl.java:55,68,100`、`SendMessageTraceHookImpl.java:57,60`、`EndTransactionTraceHookImpl.java:60,63`、`DefaultRecallMessageTraceHook.java:63,64`；remoting `protocol/NamespaceUtil.java` 的兩個 `withoutNamespace` overload。

三個 Rust trace helper 都委派給 `ClientConfig::without_namespace`，只移除設定中的 namespace。Java trace hook 呼叫單參數 `NamespaceUtil.withoutNamespace`，不依賴 client 設定便移除資源的 namespace。因此當設定的 namespace 是 `tenant` 或未設定時，Rust 保留 `other%later-group`，Java 則輸出 `later-group`。

Lite 另有階段差異：before callback 先移除一次 namespace，`consume_contexts` 再移除一次，將同一結果放入兩列。Java 對即時 group 正規化一次以建立 `SubBefore`，再對保存的 before group 正規化一次以建立 `SubAfter`。巢狀名稱下，這兩列本來就可能不同。

**已執行的 Lite 矩陣**（`before / after`）：

| 設定的 namespace | 啟動後的即時 group | Java trace group | Rust trace group |
| --- | --- | --- | --- |
| `tenant` | `later-group` | `later-group / later-group` | `later-group / later-group` |
| `tenant` | `other%later-group` | `later-group / later-group` | `other%later-group / other%later-group` |
| 未設定 | `other%later-group` | `later-group / later-group` | `other%later-group / other%later-group` |
| `tenant` | `tenant%tenant%later-group` | `tenant%later-group / later-group` | `later-group / later-group` |

Facade setter 與 Java consume context 保留傳入的即時 group；正規化屬於 tracing。Java group 驗證允許 `%`。這是合法輸入的輸出不一致，與 executor 或 library 選擇無關。

**由原始碼確認的相關路徑：**一般 Consumer trace 對 before/after group 與 bean topic 使用受設定限制的 namespace helper。Producer Pub、EndTransaction、Recall trace 也對 group/topic 欄位使用相同受限操作；Java 對應流程使用不受設定限制的操作。這些相關路徑已檢查原始碼；本輪實際執行的 wire 比對僅涵蓋 Lite。

**修正前輪驗收用語：**保留 before 階段身分，是指 after 狀態取自 before 快照，不代表所有合法名稱的 before/after 字串都必須相同。前輪 plain／單層 prefix 案例掩蓋了 Java 的第二次正規化。應符合上表各階段的 Java 結果，並保留已修復的即時 group 擷取邊界。

## 4. 實作與驗收

1. 在對應 Java 單參數 overload 的 Consumer/Lite/Producer trace 呼叫點，使用既有 `NamespaceUtil::without_namespace`。涵蓋 group 與 topic 欄位，包括 Pub、consume、EndTransaction、Recall。保留 `ClientConfig::without_namespace` 與依 namespace 設定處理的 facade/RPC 轉換；它們有不同契約。
2. Lite 擷取經過一次正規化的 nullable before group。`SubBefore` 使用此快照值，`SubAfter` 則對快照值再做一次不受設定限制的操作。不可在建構 before 列之前就正規化兩次，也不可重新讀取後續 facade group。調整 `consume_contexts` 與測試，使直接 helper 與已安裝 hook 路徑遵守相同的階段契約。
3. 補上 Java 比對：未設定／已設定 namespace、plain／相符／其他／巢狀 prefix、可接受時的 null／空值、retry/DLQ prefix 與 system resource。驗證每個受影響 hook 的 group 與 bean-topic 值，並保留至少一個實際 Lite SEND 比對。未設定 namespace 時，公開 Producer group `other%workers` 必須 trace 為 `workers`，也要比對 Pub topic 正規化。
4. 保留 G01 framing／大小／投遞／commit 回歸與 G02 即時 group／null-poll 測試。Callback 間變更 group 時，after 列仍須從已擷取的 before group 衍生。持續涵蓋 message ID、region、TRACE_ON 篩選、access channel、status 與 request 關聯。
5. 完成聚焦 trace 測試與固定版本 Java 比對後執行：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

驗收須符合完整矩陣、各階段語義與所有受影響 trace helper，且不變更 consumer registration 或 offset ownership。目前通過的 2,067 個測試為不回歸基準。Probe assertion 目前描述已觀察到的差異；將證據轉為修復驗證時，須改成 Java 預期值。既有依環境而定的 live trace 情境，可在環境可用時最後重跑，補充確定性的 namespace 矩陣。
