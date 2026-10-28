# 第五輪一致性 review：trace 身分、關閉與失敗隔離

## 1. 基準、範圍與結果

日期：2026-10-28。Review revision：`dbbabeb0cb88c503ff79182f2497b5252eb1c3c1`。檢查的 production 變更為 `83536bab` 與 `400774e1`。沿用公開 API／輸入／設定，以及執行流程邏輯須與 Java RocketMQ client 5.5.1 一致的契約，允許使用 Rust 原生組件。

本輪檢查第四輪修復的完整差異，以及相鄰 trace hook、公開 group 變更、訊息／trace 發送、shutdown 與編碼失敗處理。未重新窮盡稽核未變更的 Protocol/Remoting 與其他子系統。固定版本 client source JAR SHA-256 為 `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`。

**確認 3 項新問題：1 項 P1、2 項 P2。** H01 的 namespace 案例已通過；下列問題涉及不同的擷取與生命週期邊界。本輪僅新增文件與本機證據，保留既有紀錄與 production code。

| ID | 優先級 | 問題 |
| --- | --- | --- |
| I01 | P2 | Producer Pub/EndTransaction trace 使用啟動時 group，未使用當次 hook context |
| I02 | P1 | 一般／Lite trace shutdown 在 timeout 之外等待，耗時隨排隊發送量增加 |
| I03 | P2 | 一筆 trace 編碼失敗便終止一般 dispatcher，拒收後續合法 trace |

P1 表示 Broker 故障時，服務 shutdown 延遲會隨 trace 積壓量成長。P2 表示錯誤 trace 歸屬，或單一批次失敗後持續喪失 tracing。

## 2. 驗證與證據

| 檢查 | 結果 |
| --- | --- |
| H01 namespace 矩陣 | 對此 revision 重播修正後 probe：4 組 Lite 其他／巢狀／已設定／未設定 namespace 案例，before/after group 均符合固定版本 Java，Rust 使用實際 trace SEND 列。 |
| 第四輪實作 | Consumer/Lite/Producer helper 已改用不受設定限制的 NamespaceUtil 操作；Lite 正確分開 before 與 after 正規化。Group/topic 回歸測試通過。 |
| Workspace | `cargo test --workspace --offline` exit 0：含 doctest 與先前 LZ4/Lite 回歸，共 94 個結果摘要，**2,067 passed、0 failed、0 ignored**。 |
| 新 probe | 8 次 Rust/Java 執行均完成，assertion 確認下列 3 項差異。 |

本機證據遵守專案忽略 `/evidence/` 的慣例：[結果](../../../evidence/plan/2026/10/06-project-parity-fifth-review-evidence/results.json)、[前輪案例重播](../../../evidence/plan/2026/10/06-project-parity-fifth-review-evidence/previous-review-check.json)、[workspace 摘要](../../../evidence/plan/2026/10/06-project-parity-fifth-review-evidence/workspace-test-summary.json)。結果保留 revision、source/probe hash、toolchain、stdout/stderr 與執行限制。證據僅留在本機，不可 stage 或 commit。

使用已快取的 Rust/Maven 依賴、JDK、Maven 與專案 toolchain 重現：

```sh
python3 evidence/plan/2026/10/06-project-parity-fifth-review-evidence/run_probes.py --output /tmp/rocketmq-fifth-review-results.json
```

Rust 使用 production library 與 scripted loopback NameServer/Broker peer。身分檢查讀取實際 business 與 trace SEND；EndTransaction 使用公開 hook 執行 helper，不發送 transaction Broker RPC。Java 使用人工建立的 send context/result 與反射注入的 client factory，呼叫實際 hook。Shutdown 比較 Rust 實際網路發送停滯，與 Java 實際 dispatcher 關閉注入阻塞 task 的 executor；這是不同的受控 fixture，不是真實跨 client outage 測試。失敗隔離呼叫 Java 實際 AsyncDataSendTask/executor 與 queue API，未啟動內部 producer，因此不宣稱 Java trace 已投遞 Broker。本輪未執行真實 RocketMQ 服務、完整 Maven fixture verify、Clippy 或跨平台檢查。

## 3. 問題與改善要求

### 3.1 I01 — Producer trace 略過當次 hook group

**位置：**[啟動時安裝](../../../crates/rocketmq-classic-producer/src/producer.rs#L1249)、[Pub group](../../../crates/rocketmq-classic-producer/src/trace.rs#L171)、[EndTransaction hook](../../../crates/rocketmq-classic-producer/src/trace.rs#L228)、[即時 setter](../../../crates/rocketmq-classic-producer/src/producer.rs#L1011)、[當前 end-transaction context](../../../crates/rocketmq-classic-producer/src/producer.rs#L311)。Java：`trace/hook/SendMessageTraceHookImpl.java:57`、`EndTransactionTraceHookImpl.java:60`、`impl/producer/DefaultMQProducerImpl.java:971,1198`。

兩個 Rust trace hook 保存啟動時 `producer_group`，使用它取代 `context.producer_group`。Java 從每次 callback 的 context 讀取 group。Namespace 正規化已正確，但來源值仍舊過時。

**已執行案例：**以 `initial-producer` 啟動 tracing-enabled Producer，呼叫 `set_producer_group(Some("later-producer"))`，發送一筆訊息並執行其 end-transaction hook。Rust business SEND header 與兩個 hook context 都使用 `later-producer`，Pub 與 EndTransaction trace 列卻使用 `initial-producer`；Java hook 的兩種 trace group 均為 `later-producer`。第二次執行在啟動前註冊 custom hook，將 callback group 改成 `hook-producer`：Rust context 顯示該值，trace 仍是 `initial-producer`；Java trace 使用 `hook-producer`。

**改善：**在對應 Java 的擷取階段，從實際 nullable hook context 取得各 trace group。Pub 保留 before 快照供配對 after callback 使用；EndTransaction 使用當次 context。移除這些判斷對啟動 group 快照的依賴；不變更內部 trace producer 身分、runtime registration 或 business RPC header 擷取邊界。

**驗收：**Pub 與 EndTransaction 都涵蓋啟動後 group 變更、較早 hook 的變更，並透過可呼叫的 hook API 測試 null，以及 plain／其他／巢狀 namespace。分別驗證實際 Pub/EndTransaction trace SEND 列、hook context 與 business SEND group。保留 H01 正規化與 Pub before/after 關聯。

### 3.2 I02 — Trace shutdown 的 drain 位於 timeout 之外

**位置：**[一般 shutdown](../../../crates/rocketmq-classic-producer/src/trace.rs#L579)、[Lite shutdown](../../../crates/rocketmq-classic-consumer/src/lite/trace.rs#L432)、[循序 trace 發送](../../../crates/rocketmq-classic-producer/src/trace.rs#L748)，以及 Producer `shutdown_trace_dispatcher`、Push `shutdown_trace_dispatcher`、Lite `shutdown` 的 owner 關閉路徑。Java：`trace/AsyncTraceDispatcher.java:205`、Common `utils/ThreadUtils.java:185`。

兩個 Rust shutdown 都先等待 `Shutdown` 進入 channel，再等待 oneshot reply，之後才啟動一般 5 秒或 Lite 6 秒的 worker join timeout。Reply 在先前 trace 發送與內部 producer shutdown 完成後才送出，因此宣告的 timeout 不限制前述等待。Command queue 滿載又增加一個不受 grace 限制的 admission wait。循序 worker 等待每次發送自己的 deadline，整體 drain 耗時會隨排隊工作量增加。

**已執行案例：**設定 trace batch size 1，排入 4 個合法 context，等到第一個 trace SEND 後不回覆。一般與 Lite shutdown 在 12 秒後都仍未完成；恢復回覆後，兩者才於約 12 秒完成。Java 實際 dispatcher shutdown 面對 trace executor 內兩個可中斷的 30 秒 task，約 5 秒返回並終止該 executor。Java 原始碼採用共用的 5 秒 graceful executor wait，再加上有界的 5 秒 cancellation wait，不會循序等待每個排隊 send 各自完成。

**影響：**Producer/Push/Lite shutdown 在 outage 時承受此 trace 延遲，大量 trace 排隊會使服務終止遠超過整體 grace interval。執行結果證明 12 秒邊界；更長積壓的延遲放大由原始碼推導，未實測數小時。

**改善：**依 Java 的整體等待政策實作有界 drain/cancel 流程，涵蓋 control message admission、pending send、reply wait、worker 終止與內部 producer 清理。跨階段使用共用 deadline，不可對每個排隊 batch 重啟完整 grace period。到期後取消剩餘工作、join worker，並停止／註銷所擁有的 trace producer。保留 worker ownership 或使用 cancellation guard，避免 shutdown flag 設定後，丟棄 shutdown future 會使 worker 脫離管理。提早發生的 channel/worker 錯誤也必須進入清理。

**驗收：**兩個 dispatcher 都測試停滯 Broker send 下的單一／多個 batch，以及飽和 trace queue。終止時間須有界且不隨積壓量增加，停止接受新 context，不留下所擁有的 task／producer registration，並能安全重複 shutdown 或取消呼叫端。驗證 Producer、Push、Lite owner 路徑。Broker 正常回覆時，仍須在終止前 flush 已排隊的合法 trace。

### 3.3 I03 — 一個編碼失敗批次永久停止一般 dispatcher

**位置：**[未隔離的編碼](../../../crates/rocketmq-classic-producer/src/trace.rs#L755)、[worker loop](../../../crates/rocketmq-classic-producer/src/trace.rs#L611)、[nullable SubAfter access channel](../../../crates/rocketmq-classic-core/src/trace.rs#L410)。Java：`trace/AsyncTraceDispatcher.java:289,304`、`TraceDataEncoder.java:208`。

公開的一般 dispatcher 接受 SubAfter access channel 為 null 的 TraceContext。Encoder 在 Java 會拋出例外的位置刻意 panic。Rust 直接在唯一的長駐 worker 內編碼，未建立失敗隔離邊界；Java 則透過 executor 提交各 AsyncDataSendTask，編碼例外只讓當次 task 失敗，dispatcher/executor 仍可使用。

**已執行案例：**Append 一個 region/topic/bean 合法、但沒有 access channel 的 SubAfter context，再 flush 並 append 合法 SubBefore context。Rust worker panic，flush 回傳 `WorkerUnavailable`，後續 append 回傳 `false`。Java 實際 task 以 `NullPointerException` 失敗，後續 append 仍回傳 `true`，flush 後續工作後 executor 仍可正常終止。Java fixture 未驗證該後續 trace 的 Broker 投遞。

**影響：**一個格式不完整的 context，便永久停用 Producer 或 Push trace 路徑共用的一般 dispatcher。這與 Java 的 task 失敗隔離不同，不需要改變公開 encoder 的錯誤行為。

**改善：**在對應 Java appender task 的 batch 邊界隔離序列化／task 失敗，持續處理後續排隊工作。可使用內部可回傳錯誤的 encoder，或精準的 unwind 邊界，同時保留公開 encode 語義。若真的發生終止性失敗，仍須可靠清理 worker 與所擁有的 producer。不可因 trace 編碼失敗改變 business operation 的結果。

**驗收：**比對 Java/Rust 格式錯誤批次，以及後續獨立排入的合法 batch；合法 append/flush 必須成功，並觀察到該 trace SEND。涵蓋缺少 access channel、Pub/EndTransaction 必要 bean 欄位，以及錯誤／合法混合積壓與最終 shutdown 清理。保留既有合法 trace 編碼及 namespace/group 行為。

## 4. 實作順序與驗證

1. 修復 I02 兩個 dispatcher 的 shutdown ownership 與共用 drain/cancel deadline，涵蓋失敗／取消清理。
2. 修復 I03 一般 appender 的失敗邊界，驗證失敗批次後仍能處理合法工作。
3. 修復 I01 Pub/EndTransaction context 來源，保留 H01 不受設定限制且依階段執行的正規化。
4. 執行聚焦 Java/Rust 身分、停滯 shutdown 與失敗隔離 probe；將證據 assertion 從目前缺陷改成符合 Java 的結果，再執行：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

目前通過的 2,067 個測試為不回歸基準。保留先前 LZ4 投遞／commit、nullable Lite poll、即時 group 與 H01 trace 案例。依環境而定的 live trace/outage 檢查可在環境可用時最後執行；確定性的 loopback 與固定版本 Java 測試，可在不需要 production 憑證、也不仿製 Java executor 組件的情況下驗證這些 client 邊界。
