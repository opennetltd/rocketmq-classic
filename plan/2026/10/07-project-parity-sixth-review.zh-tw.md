# 第六輪一致性 review：trace bean 選取

## 1. 基準、範圍與結果

日期：2026-10-28。Review revision：`1ceff1eb64660e7505ec04ce70d9d138b1157aea`。本輪檢查第五輪修復完整差異（`672139dc`、`35f1ae96`、`3314237b`、`224b5c26`），以及相鄰的 Core trace 編碼、Producer/Consumer dispatcher 行為、hook context 擷取與共用 runtime shutdown。未重新窮盡稽核未變更的專案子系統。

沿用公開 API／輸入／設定，以及執行流程邏輯須與 Java RocketMQ client 5.5.1 一致的契約，允許使用 Rust 原生組件。固定版本 client source JAR SHA-256 為 `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`。

**確認 1 項新的 P2 問題：J01，非 consume trace 編碼會處理 Java 不序列化的 bean。** 這涉及公開 TraceContext/dispatcher API 可接受的輸入範圍，不是 I01–I03 修復失效。本輪僅新增文件與本機證據；production code 與先前紀錄保持不變。

## 2. 前輪修復驗證與證據

| 檢查 | 此 revision 的結果 |
| --- | --- |
| I01，callback group | 重播 Java/Rust probe：Pub 與 EndTransaction 使用 `later-producer`，或使用較早 hook 變更後的 `hook-producer`。Business SEND 獨立保留 `later-producer`。 |
| I02，有界 shutdown | Batch size 1、4 的 trace send 停滯時，一般與 Lite owner shutdown 均約 5 秒完成，內部 trace producer 進入 ShutdownAlready。滿 queue／呼叫端取消與 owner 回歸測試通過。 |
| I03，失敗隔離 | 格式不完整 context 的 flush 成功；後續合法 append/flush 成功並發出 trace SEND。最終 shutdown 會停止內部 producer。J01 涉及 Java 可讀的 context 被錯誤歸類為格式錯誤。 |
| 共用 runtime | 額外 loopback probe 關閉 trace dispatcher 後，host Producer 仍為 Running，且成功完成後續 business SEND。 |
| Workspace | `cargo test --workspace --offline` exit 0：含 doctest 共 94 個結果摘要，**2,072 passed、0 failed、0 ignored**。 |

本機證據遵守忽略 `/evidence/` 的慣例：[新結果](../../../evidence/plan/2026/10/07-project-parity-sixth-review-evidence/results.json)、[前輪案例重播](../../../evidence/plan/2026/10/07-project-parity-sixth-review-evidence/previous-review-check.json)、[workspace 摘要](../../../evidence/plan/2026/10/07-project-parity-sixth-review-evidence/workspace-test-summary.json)。結果保留 revision、source/probe hash、直接 encoder 輸出、實際 Rust trace 列與執行邊界。不可 stage 或 commit 證據。

使用已快取的 Rust/Maven 依賴、JDK、Maven 與專案 toolchain 重現：

```sh
python3 evidence/plan/2026/10/07-project-parity-sixth-review-evidence/run_probes.py --output /tmp/rocketmq-sixth-review-results.json
```

Java 直接呼叫固定版本 `TraceDataEncoder.encoderFromContextBean`；Rust 直接呼叫 production `TraceContext::encode`，並透過 production 一般 dispatcher 發送至 scripted loopback peer。不宣稱已使用真實 RocketMQ 服務或驗證 Java trace 的 Broker 投遞。本輪未重跑完整 Maven fixture 驗證、Clippy 與跨平台檢查。

## 3. J01 — Pub、EndTransaction、Recall 序列化整個 bean 清單

**優先級：P2。位置：**[共用編碼迴圈](../../../crates/rocketmq-classic-core/src/trace.rs#L378)、[Pub 必要欄位](../../../crates/rocketmq-classic-core/src/trace.rs#L380)、[EndTransaction 必要欄位](../../../crates/rocketmq-classic-core/src/trace.rs#L433)、[dispatcher 錯誤處理](../../../crates/rocketmq-classic-producer/src/trace.rs#L842)、[transfer key](../../../crates/rocketmq-classic-core/src/trace.rs#L473)。Java 原始碼：client `trace/TraceDataEncoder.java:168,217,234,249`。

Rust 將五種 trace type 都放在同一個 `for bean in trace_beans` 迴圈內。Java 的 Pub、EndTransaction、Recall 僅序列化 `traceBeans.get(0)`，SubBefore/SubAfter 才序列化每個 bean。Java 另行對所有 trace type 的每個 bean 收集 transfer key。這是兩種不同的選取規則。

Probe 建立兩個 bean，message ID 為 `first`／`second`，key 為 `first-key`／`second-key`。Context timestamp、request ID、region、group 與第一個 bean 的必要欄位固定，供位元組比對。

| 輸入 | Java encoder | Rust encoder | Rust dispatcher 實際輸出 |
| --- | --- | --- | --- |
| Pub，兩個完整 bean | `first` 的 1 列 | 2 列 | 2 列 |
| EndTransaction，兩個完整 bean | `first` 的 1 列 | 2 列 | 未另行重播 wire |
| Recall，兩個完整 bean | `first` 的 1 列 | 2 列 | 未另行重播 wire |
| Pub，第一個完整；第二個沒有 message type | `first` 的 1 列 | 對未使用的第二個 bean panic | I03 隔離 panic 後輸出 0 列 |
| EndTransaction，第一個完整；第二個沒有 message type/state | `first` 的 1 列 | 對未使用的第二個 bean panic | I03 隔離 panic 後輸出 0 列 |
| SubBefore/SubAfter，兩個完整 bean | 2 列 | 位元組一致的 2 列 | SubBefore：2 列 |

完整 bean 案例中，兩邊皆收集 4 個 transfer key，包含第二個 bean 的 ID/key。證據標籤 `invalid_tail` 表示後續 bean 缺少必要序列化欄位；Java 接受該 context，因為它根本不序列化那些後續欄位。必要的第一個 bean 維持完整。

**影響：**透過公開 API 建立的多 bean 非 consume context，在 Rust 會產生額外 trace 記錄，或遺失整筆合法的第一個記錄。I03 已正確維持 dispatcher 存活，但無法修正 encoder 讀取 Java 選取範圍以外欄位的問題。預設內建 Pub/EndTransaction hook 通常只建立一個 bean，因此本問題不代表一般單 bean 發送必定重複 trace，也不影響 business message 投遞。

**實作：**依 TraceType 選取序列化 bean：Pub/EndTransaction/Recall 僅第一個，SubBefore/SubAfter 使用全部。重用既有記錄格式，不讀取或驗證未選取 bean 的序列化欄位。Transfer key 仍從原始完整清單收集；若直接將整個 context/list 限制為一個 bean，雖然修正列數，卻會破壞 key 行為。保留對應 Java 的空清單／null／必要首筆欄位失敗，以及 I03 對真正無效選取資料的隔離。Lite 的 consume-only encoder 繼續處理全部 consume bean。

## 4. 驗證與驗收

1. 擴充 `compat/java-reference/src/main/java/compat/TraceFixtures.java` 與 Core trace fixture 測試，涵蓋五種 trace type 的零／一／兩個 bean。加入僅後續 Pub/EndTransaction bean 缺少 message type/state，以及被選取的第一個 bean 缺少必要欄位的案例。透過實際 Java 呼叫固定預期記錄與 transfer key。
2. 驗證非 consume encoder 僅輸出第一列，忽略後續僅供序列化的欄位，並保留所有 bean 的 ID/key。Consume encoder 仍輸出全部列，delimiter、欄位、access-channel 行為與關聯資料的位元組一致。
3. 新增一般 dispatcher loopback 回歸：Java 接受的 Pub/EndTransaction context 必須輸出合法首筆記錄，不可被丟棄；多 bean Pub/Recall 不可產生額外記錄。在真正無效的選取記錄後加入合法工作，保留 I03 失敗隔離。保留 I01 callback group、H01 namespace 處理，以及 I02 shutdown／清理／共用 runtime 行為。
4. 將本機 probe 的缺陷 assertion 改成修復後 Java 結果，再執行：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

驗收須符合 Java 依 type 選取記錄與收集所有 bean transfer key 的語義，維持 2,072 個測試的不回歸基準及先前修復。既有 live trace 情境可在環境可用時最後執行；scripted peer 涵蓋一般內建 hook 不會產生的自訂公開 context 形狀。不需要新增設定或仿製 Java 組件。
