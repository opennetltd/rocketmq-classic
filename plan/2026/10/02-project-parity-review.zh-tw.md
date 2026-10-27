# 專案 Review：Java 5.5.1 相容性與改善計畫

## 1. 核心原則、基準與結論

Review 日期：2026-10-27。Rust revision：`a91e91bcdcfe92b01a351c56c7ce0dd4ff393e96`；review 開始前 worktree 乾淨。本次只新增 review 文件及可重現的探測程式。

依使用者原則判定：公開 API 能力、輸入範圍與設定必須對應 Java，其餘行為須保持執行流程邏輯一致。Tokio、rustls、Rust 集合、Future 與 ownership 都是可接受的實作方式。Netty 物件、JVM 執行緒身分、Java 例外與 stack 字串格式，本身不構成必須重現的要求。

對照基準為本機快取的 `org.apache.rocketmq:rocketmq-client:5.5.1` source/binary 及其 Common/Remoting 相依套件，不使用目前 Java checkout 的版本。Client source JAR SHA-256：`de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`，與 [oracle 來源紀錄](../../../compat/java-reference/README.md) 一致。以下 `J:` 代表該 client source JAR 中 `org/apache/rocketmq/client/` 下的路徑；Common 來源則列出完整套件路徑。

**本次有十項可執行的改善發現：五項 P1、五項 P2。** 最明確的重現缺陷是合法壓縮 Pull batch 被靜默截斷：Java 交付 24 則，Rust 只交付 21 則，卻保留 `nextBeginOffset=24`。既有 audit 結案及測試通過，仍未涵蓋所有執行分支。本次 review 不代表已可發布。

P1：可能略過訊息、額外執行使用者程式、工作逃逸停機管理，或安全使用指引明顯錯誤。P2：公開 API／設定、重試、停機時間預算或證據維護缺陷。文件區分「來源已確認」與「實際執行重現」；本次沒有執行 live Broker 或正式環境工作負載。

## 2. 涵蓋範圍與驗證

| 範圍 | 檢查邊界與證據 | 結果／限制 |
| --- | --- | --- |
| Common | Message 表示方式、完整／compact 編解碼、壓縮、限制與既有 fixtures | R01；D02 |
| Protocol | Typed header 解析／檢查、Java signed number、DTO／route mapping 與 inventories | 已檢查範圍未新增確認缺陷；不代表每個 DTO／輸入均已完整證明 |
| Remoting | Frame 限制、pending request 取消、connection／GO_AWAY ownership、callback、TLS 驗證與 inventories | R09 為文件問題；部分既有 runtime 差異已有明確處置紀錄 |
| Core | RPC route fallback、共用 runtime／config、route 發布、offset RPC、admin fan-out、ACL／trace 邊界 | 檢查 R01 與 consumer 設定的下游影響；未另外確認 Core 缺陷 |
| Producer | Send retry／deadline、default-route lookup、訊息準備、transaction、request/reply、trace | 已檢查流程未新增確認缺陷 |
| Consumer | Pull／Push／Lite、offset、assignment／rebalance、concurrent／orderly／POP 派送與停機 | R01–R08；D01 |
| Facade、測試、發布紀錄 | Re-export、API crosswalk tests、integration／live 紀錄、README 與相容性狀態 | R06／R08 的 API 可呼叫性；R10 的證據一致性 |

這是全專案範圍、依風險深入的來源 review，不宣稱逐行或逐一 overload 完整證明相等。公開 API 名稱檢查、欄位複製測試、歷史 live 摘要，都按其證據限制使用，不能取代行為驗證。

本 revision 實際執行：

- `cargo test --workspace --locked`：exit 0；94 筆 test-result 摘要，共 **2,032 passed、0 failed、0 ignored**，包含 doctests。數量為各 test harness 回報結果的加總。
- Offline Java／Rust probes：exit 0。相同的 Rust 編碼 bytes 交給固定版本 Java `MessageDecoder.decodesBatch` 解碼；Rust 端使用實際的 `PullAPIWrapper::process_pull_result`。
- Java Lite 初始 offset 呼叫使用受控的 `OffsetStore` 與 `MQAdminImpl`；另直接呼叫 Pull／Lite 的 nullable group constructor。這些案例的 Rust 對照採來源分析。
- POP lifecycle 發現為來源確認；本次未新增執行 Java／Rust POP lifecycle differential。未重新執行完整 Maven fixtures、Docker／live scenarios、stress、Clippy 或跨平台測試，也未將既有歷史通過紀錄改判失敗。

[Probe 結果](02-project-parity-review-evidence/results.json) 保留 revision、hash、輸出與驗證邊界。備妥本機 Maven／Rust 相依快取、JDK、Maven 及 repository Rust toolchain 後，可重現：

```sh
python3 plan/2026/10/02-project-parity-review-evidence/run_probes.py --output /tmp/rocketmq-parity-review-results.json
```

腳本將 workspace lock 複製到外部暫存 crate，離線解析相依，不連線 Broker，不修改 production source 或 workspace lock。其 assertion 刻意偵測目前的 R01 缺陷；R01 修正後，將 probe 納入 regression coverage 時須改為正確預期值。

## 3. 發現項目

| ID | 優先級 | 發現 | 證據 |
| --- | --- | --- | --- |
| R01 | P1 | Rust 額外解碼限制靜默丟棄合法訊息，卻保留 Broker 下一個 offset | Java／Rust 執行重現及 caller 追查 |
| R02 | P1 | POP orderly 執行及確認了固定 Java 路徑不會執行的使用者工作 | 來源確認 |
| R03 | P1 | POP listener 子 task 逃逸其 owner worker 的取消管理 | 來源確認 |
| R04 | P2 | POP shutdown 對每個 worker 各給一次完整等待預算 | 來源確認 |
| R05 | P1 | Lite 初始 offset 改變 retry topic 與負 offset 分支 | Java 實際呼叫；Rust 來源對照 |
| R06 | P2 | 部分 Java live 設定在 Rust facade 建立後無法修改或被凍結 | 公開 API／讀取位置來源對照 |
| R07 | P2 | Lite pull 忽略 exception delay 設定與 Broker FLOW_CONTROL 分類 | 來源確認 |
| R08 | P2 | Pull／Lite group API 無法表示 Java 的 null 輸入 | Java 實際呼叫；Rust signature 對照 |
| R09 | P1 | README 承諾實作刻意未提供的 TLS hostname 驗證 | 來源／測試／文件對照 |
| R10 | P2 | 使用者文件與 audit 文件的目前相容性狀態互相矛盾 | 文件確認 |

### 3.1 R01 — 解碼限制造成靜默漏訊息

**位置：** [DecodeLimits 與 prefix decoder](../../../crates/rocketmq-classic-common/src/message/decoder.rs#L203)、[PullAPIWrapper](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L162)、[Push offset 推進](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L3321)、[Lite 結果接收](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1672)。Java：`common/org/apache/rocketmq/common/message/MessageDecoder.java` 的 `decodesBatch`；`J:impl/consumer/PullAPIWrapper.java` 的 `processPullResult`。

Rust 固定限制單筆 record 16 MiB、解壓 body 64 MiB、aggregate decoded data 64 MiB。`decode_many_compatible` 丟棄 `decode_many_prefix_with_limits` 回傳的錯誤，Pull wrapper 以 FOUND 回傳成功解出的 prefix，且不改 `next_begin_offset`。Java 對 malformed record 的 prefix 處理，不能作為 Rust 額外限制拒絕合法 record 的相容性依據。

**重現：** 24 筆、每筆 body 3 MiB，均低於 Producer 預設 4 MiB body 上限，壓縮後合計 76,032 bytes。Java 解出 24 筆；Rust 回傳 FOUND、21 筆、next offset 24。另一筆 65 MiB body 壓縮後為 66,312 bytes，Java 可解碼，Rust 則回傳 FOUND、零筆、next offset 1。後者須調高 sender body-size 設定，前者不需要。

**影響：** Push 在派送前已推進 pull request，後續可能持久化超過被略過訊息的 offset；Lite 推進 fetched position；explicit Pull 回傳不完整成功。本次已在 decoder／wrapper 邊界重現並以來源追查 offset 推進，未實際執行 Broker persistence 測試。POP／admin 也呼叫同一個會遺失解碼錯誤的函式，但後果不同，須各自補 regression。

**改善／驗收：** 移除對合法 Java-compatible batch 的未公開固定限制，或重整解碼／交付方式以保留全部 record。若內部仍保留資源保護，須回傳可辨別的失敗，且不可推進 offset 或 ACK 未處理 record；Java malformed-input 行為另行保留測試。不要為掩蓋此差異新增無 Java 對應的公開設定。24 筆案例必須交付 24 筆，並補 Push／Lite offset、POP／query 測試，證明合法 suffix 不會以成功狀態消失。

### 3.2 R02 — POP orderly 的執行行為與 Java 不同

**位置：** [POP 派送](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5375)、[listener 處理](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5425)。Java：`J:impl/consumer/ConsumeMessagePopOrderlyService.java:200,354`。

固定版本 Java 的 `submitPopConsumeRequest` 建立 request 時沒有保留訊息列表；`ConsumeRequest.run` 檢查 dropped 狀態、取得 sharding-lock 物件後便結束，不會呼叫 orderly listener 或 ACK 這些訊息。Rust 將列表拆 batch、建立 worker、呼叫 orderly listener，並在成功時 ACK；這些 worker 也沒有一般 orderly 路徑的 per-process-queue consume gate。這是額外可觀察的執行行為，不只是 executor 實作不同。

**改善／驗收：** 相容路徑應恢復固定 Java 的流程，不可把功能擴充版 POP orderly 當作 Java 5.5.1 parity。補上 non-dropped POP queue 的直接 differential，核對 listener 次數、hooks 與 ACK／visibility requests。固定版本路徑須為零次 listener 呼叫，且沒有根據 listener 結果發出的 ACK。若要提供可運作的 POP orderly，應另外記錄產品決策與 ordering contract。

### 3.3 R03 — POP listener Future 在 worker 停機後仍存活

**位置：** [POP callback spawn](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5464)、[POP worker shutdown](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L7096)。對照 [一般 concurrent callback ownership](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L3670)。

POP 直接建立 `tokio::spawn` listener handle 後 await。Shutdown 只 abort `pop_consume_workers` 中的外層 handle；Tokio JoinHandle 被 drop 時，子 task 會 detach。因此 cooperative listener 仍可能在 shutdown 後產生應用程式副作用，但處理其完成／ACK 的 owner 已消失。一般 Pull concurrent／orderly 已使用 `AbortOnDrop` 包住 listener handle。

**改善／驗收：** POP 套用既有的 owned-child 模式，並檢查 direct-consume callback 是否有相同 ownership 問題。使用帶 drop sentinel 的阻塞 async listener：worker 被 abort 時 listener 須取消／drop；shutdown 後解除 barrier，不得再產生副作用或 ACK。保留既有 panic／result 分類及已完成工作的處理。不承諾強制終止任意 blocking 使用者程式。

### 3.4 R04 — POP 停機等待隨 worker 數量倍增

**位置：** [shutdown worker loop](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L7102)。Java：`J:impl/consumer/ConsumeMessagePopConcurrentlyService.java:87`；`common/org/apache/rocketmq/common/utils/ThreadUtils.java:185`。

Rust 在 sequential loop 裡對每個 worker 重新套用 `timeout(await_termination_millis, worker)`。N 個持續 pending 的 worker，POP 等待可能接近 N × T，後面還有其他 shutdown cleanup。Java 對整個 executor 等待 T，要求取消，再對整個 executor 等待 T，並非每個工作重新獲得預算。

**改善／驗收：** POP service 每個 phase 只建立一次 deadline，集體等待／取消 workers。依 Java graceful 與 cancellation phases 計算，不為各 worker 續發時間。使用 paused time 比較 1 與 20 個 stalled workers，T=100 ms 時 POP 等待須受 service phases 限制，至多 2T 加 bounded cleanup，不隨 N 倍增。與 R03 一併驗證，避免 detached listener 造成虛假的時間界線。

### 3.5 R05 — Lite 初始 offset 分支不一致

**位置：** [initial_offset](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1641)。Java：`J:impl/consumer/RebalanceLitePullImpl.java:89`。

| Offset lookup 後的輸入 | Java | Rust |
| --- | --- | --- |
| LAST、缺 offset `-1`、`%RETRY%…` topic | 從 0 開始 | 查 MAX_OFFSET |
| TIMESTAMP、缺 offset、retry topic | 直接查 MAX_OFFSET | 查 SEARCH_OFFSET，只有負值才 fallback MAX |
| TIMESTAMP、一般 topic、SEARCH_OFFSET 回傳 `-1` | 保留 `-1`，不以此開始有效 pull | 改用 MAX_OFFSET |
| 支援的 policy、custom store 回傳低於 `-1` | 回傳無效／不可用的 `-1` | 當成缺少 offset，執行初始化 policy |

Java probe 確認 retry LAST=0、不可用 stored offset 保持無效、一般 TIMESTAMP search `-1` 仍為 `-1`。Retry TIMESTAMP 的 request 選擇也直接記載於固定版本來源。Rust fallback 可能跳過 retry 歷史，或在 Java 會重試初始化時直接從尾端開始。

**改善／驗收：** 各支援 policy 區分 stored-valid、missing（`-1`）、unavailable（`< -1`），套用 Java retry-topic 特例，保留成功查詢的負 search 結果。新增 Lite 專屬 invoked matrix，以 request counter 區分 MAX／SEARCH，涵蓋三種支援 policy、retry／一般 topic、stored `>=0`／`-1`／`-2`、RPC failure 與 negative search。不能用既有 Push 初始 offset fixture 代替 Lite。

### 3.6 R06 — Live 設定缺少可呼叫的 Rust 修改路徑

**位置：** [Lite config setters](../../../crates/rocketmq-classic-consumer/src/lite/config.rs#L289)、[Lite 唯讀 config](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L204)、[Lite 初始 offset 讀取](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1641)、[Pull config](../../../crates/rocketmq-classic-consumer/src/config.rs#L98)、[Pull timeout 讀取](../../../crates/rocketmq-classic-consumer/src/pull/consumer.rs#L1450)。Java：`J:consumer/DefaultLitePullConsumer.java:575,588`；`J:impl/consumer/RebalanceLitePullImpl.java:90`；`J:consumer/DefaultMQPullConsumer.java:212,228,236`。

Lite 的 `consume_from_where`、`consume_timestamp` setter 需要可變 config，但 config 已 move 到 consumer；`config()` 只回傳 `&LitePullConsumerConfig`，consumer 也沒有等價 setter。Java 在初始化新 assigned queue 時會讀取這些值，因此 running 中修改可影響後續 queue。Pull 的 suspend／pull timeout 設定也有相同 ownership 問題，即使後續 pull 仍會讀取設定。只有建構前 builder 的 setter 名稱，無法保留這些 live 能力。

**改善／驗收：** 依 Java 實際讀取／capture 時點，提供 synchronized facade setter 或保留的 live config handle。不要因此動態重建 constructor-captured executor。新增外部 compile-and-run 案例：facade 建立後可修改；running 時先改 Lite timestamp／policy 再新增 queue、先改 Pull timeout 再呼叫下一次 pull，與 Java request 對照。Inventory test 須驗證 receiver 可存取性與 runtime 效果，不只找方法名稱或檢查欄位複製。

### 3.7 R07 — Lite 忽略 exception retry 設定

**位置：** [pull_loop](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1541)，尤其 pull error 後固定 sleep 一秒。Java：`J:impl/consumer/DefaultLitePullConsumerImpl.java:168,1002`。

Java 在 implementation 建構時 capture inherited `pullTimeDelayMillsWhenException`，一般 pull failure 使用此值；Broker FLOW_CONTROL 另走 20 ms 分支。Rust 雖將 ClientConfig 傳入 Pull engine，Lite worker 對所有錯誤仍固定等一秒。將公開 delay 設成 4,500 ms 沒有相應效果，flow control 也使用錯誤的重試分類。

**改善／驗收：** 在對應時點保留 Java delay，重新排程前檢查 typed nested Broker error。使用 paused-time scripted tests，涵蓋一般錯誤 4,500 ms、預設錯誤 1,000 ms、FLOW_CONTROL 20 ms，以及取消與成功後立即續拉。允許 scheduler 誤差；要求是設定分支／預算一致，不是執行緒時間完全相同。

### 3.8 R08 — 缺少 nullable Pull／Lite group 輸入

**位置：** [Pull group constructor／setter](../../../crates/rocketmq-classic-consumer/src/config.rs#L47)、[Lite group constructor／setter](../../../crates/rocketmq-classic-consumer/src/lite/config.rs#L82)。對照 [Push nullable setter](../../../crates/rocketmq-classic-consumer/src/push/config.rs#L197)。Java：`J:consumer/DefaultMQPullConsumer.java`、`J:consumer/DefaultLitePullConsumer.java` 的 constructors 與 `setConsumerGroup`。

兩個 Java facade 都接受 null String group，並保留到 startup validation 前。Probe 以 `(String)null` 建構兩者，getter 均為 null。Rust Pull／Lite group 只有 `impl Into<String>`，沒有 nullable constructor 或 nullable setter；Push 與 Producer 已使用 `Option<String>` 表示此輸入。

**改善／驗收：** 依專案既有慣例提供明確 nullable Rust adapter，盡可能保留常用 non-null convenience API。區分 null 與 empty string，validation 保持 Java 對應生命週期時點。對 null、empty、default、valid group 實際呼叫 constructor、getter／setter、namespace／start 與 failure-state 案例；不可把 Java null 換成 Rust empty string。

### 3.9 R09 — TLS 指引承諾不存在的 hostname check

**位置：** [README transport 指引](../../../README.md)、[TlsConfig contract](../../../crates/rocketmq-classic-remoting/src/client/tls.rs#L270)、[ChainOnlyServerVerifier](../../../crates/rocketmq-classic-remoting/src/client/tls.rs#L857)、[既有 TLS test](../../../crates/rocketmq-classic-remoting/tests/transport.rs#L2387)。

README 宣稱 `test_mode: false, auth_server: true` 會啟用 CA 與 hostname checks。實作刻意忽略 `_server_name`，只驗證 chain／validity，以符合 Java 5.5.1 未啟用 endpoint identification 的行為。測試與 Known Differences 正確記錄了接受 trusted wrong-host certificate。只依 README 操作的使用者會誤以為取得了實作未提供的更強驗證保證。

**改善／驗收：** 修正 README 與所有使用／安全指引，清楚區分 chain／validity verification 與 endpoint identification，連結既有 Java 行為驗證紀錄。保留 Java-compatible 預設行為；新增 hostname enforcement 不是此文件缺陷的修正。搜尋所有 TLS 指引中的矛盾宣稱，並明列 trusted wrong-host／untrusted-CA 的測試預期。

### 3.10 R10 — 目前狀態與歷史證據混在一起

**位置：** [README](../../../README.md)、[Known Differences](../../../docs/compatibility/KNOWN_DIFFERENCES.md)、[Phase 9 matrix](../../../docs/compatibility/PHASE_9_OFFLINE_METHOD_MATRIX.md)、[Phase 13 matrix](../../../docs/compatibility/PHASE_13_LITE_POP_MATRIX.md)、[release checklist](../../../docs/release-checklist.md)、[current-tree audit](../../../docs/compatibility/evidence/owner/MAN-05/current-tree-audit-20261026.md)。

README 仍稱 P18-V01 partial、P18-V04 open／current Ubuntu CI 未驗證、aggregate local gate open。較新的 Known Differences／current-tree 紀錄則稱 broad audit 已結案、local aggregate 通過、37 scenarios 均有通過紀錄，且 CI 已從要求移除。部分 phase matrix 仍標示 live gate pending。歷史紀錄也明說未保留 raw live output，本次無法獨立重查這些摘要。

**改善／驗收：** 建立一份精簡、依 source revision、oracle version、scenario result、evidence location 定位的目前狀態，連結有日期的歷史紀錄，不改寫歷史事實。對齊 README／matrix 的 current summary，舊 pending row 標成歷史或已被後續紀錄取代。將 R01–R09 新增為 open findings，不因先前結案便宣稱目前程式沒有缺陷。未來重跑須保留可重現的 raw evidence／checksum。本項不要求恢復 GitHub Actions，也不要求在 review 階段重跑 owner-only 正式環境情境。

## 4. 設計追蹤項目，非已確認相容性缺陷

### 4.1 D01 — 驗證 consume backlog 的 bounded admission

[ConcurrentConsumeService](../../../crates/rocketmq-classic-consumer/src/consume/concurrent.rs#L20) 使用固定 256-entry channel；滿載時 [PushConsumer](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L3434) 將 batch 放回，另排延遲重試。Java 使用 unbounded `LinkedBlockingQueue`。只要能保留交付／進度行為，bounded Rust queue 是合理元件選擇。本次確認額外的 admission 分支，但未證明它造成漏訊息或不可接受的 ordering／progress 差異。

後續以 blocked listeners 與超過 256 個 ready batches 驗證：沒有遺失／重複 in-flight batch、最終可前進、各 queue 能公平參與、retry task 數量有界。只有證據顯示流程缺陷時才調整 pending-work／admission，不應只為結構相似而複製 Java executor。

### 4.2 D02 — 刻意重現的 malformed-input 無窮迴圈應隔離

[Common count_inner_messages](../../../crates/rocketmq-classic-common/src/message/compact.rs#L128) 對 zero-size 或 cyclic negative-size 輸入可能持續迴圈；Known Differences 記錄測試刻意重現 Java 行為。這**不是 Java／Rust 差異**；在本次檢查範圍中，除公開 helper 外未找到 production call site。對直接處理不可信 bytes 的 caller，這是設計／hardening 考量。

可評估具 forward-progress 檢查的內部 parser；若改動公開 malformed-input contract，需明確記錄。Legacy 行為維持 bounded subprocess probe，不在 test runner 或 Tokio worker 中直接餵入這些案例。應先處理 delivery／lifecycle 缺陷，再考慮進一步重現 JVM 特定的 malformed-input 異常行為。

## 5. 實作順序與驗收關卡

1. **訊息完整性：** R01，再 R05。修改前補最小 failing behavioral regression；驗證完整交付，以及 guard／error 路徑不推進 offset。
2. **POP 流程／生命週期：** R02、R03、R04。確立固定版本 orderly 分支、listener ownership／取消與 collective shutdown budget；另測 concurrent POP，避免 orderly 分支修正掩蓋 R03／R04。
3. **公開設定與輸入：** R06、R07、R08。建立可實際呼叫的 mapping，記錄型別／nullability、允許生命週期、Java read boundary、Rust mutation path 與觀察到的效果。重點為執行覆蓋，不是擴大來源名稱掃描。
4. **文件與證據：** R09 可獨立修正；最終 implementation revision／結果記錄後完成 R10。D01／D02 在範圍決定或實際重現行為失敗前，與阻擋發布的缺陷數分開。

各變更執行對應 Common／Consumer／Remoting focused regression。Implementation phases 完成後執行既有 workspace gates：

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

Offline Maven 需要已快取固定版本相依。保留既有測試，以實際公開 API 呼叫、request／outcome 精確比較、offset／lifecycle assertion 補強。目前 2,032 項通過是 non-regression 基準，不是新增情境已通過的證據。

最後一個 implementation phase 才做環境相依驗證：執行受影響的既有 local 5.3.4 pull／Lite／POP scenarios，保留連結 revision 的 raw evidence。Baseline Broker 無法提供的分支，以 scripted compressed／offset 案例補足。只有實際需要正式 network／security／HA／workload 環境的檢查才交由 owner 執行；上述可用來源重現的發現不依賴那些輸入。

各 finding 須在適用時通過固定 oracle 的指定行為／驗收案例、維持公開 API／設定相容，並於 current status 記錄測試 revision 後才能結案。不要求把 Tokio／rustls 換成 Java 式元件。
