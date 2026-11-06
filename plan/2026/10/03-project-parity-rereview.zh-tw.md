# 複查：相容性修復與新發現問題

## 1. 基準與結果

日期：2026-10-27。Review revision：`9b006383fd8c81490c673244d69853b3d8ee89eb`；相對上一輪 review 的 production-source 修復為 `db941d9f`、`c0960129`、`7e0e9955`，之後是文件 commit `9b006383`。原有未追蹤的 `02-project-parity-review-evidence/` 目錄予以保留。

核心原則不變：公開 API 能力、設定與輸入範圍對應 Java RocketMQ client 5.5.1；執行流程邏輯一致，不要求使用 Java／Netty／JVM 元件。本機固定 source JAR 符合 SHA-256 `de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76`。以下 Java source path 為該 JAR 內的 `org/apache/rocketmq/client/` 相對路徑，明列 Common 路徑者除外。

**本輪新確認五項問題：兩項 P1、三項 P2。** 包含修復引入的 regression，以及先前未覆蓋的相鄰路徑。上一份文件維持原日期的 review 紀錄，新問題由本文件追蹤。本次只新增 review 文件與本機 probe artifacts，未修改 production code 或既有測試。

複查涵蓋完整修復 diff 與相鄰 Common、Consumer、Core、Producer、facade、文件流程；執行驗證聚焦訊息解碼、公開 compressor、Lite state／offset transition、POP lifecycle 計數。不宣稱重新完整證明所有未變動的 Protocol／Remoting 方法。

| ID | 優先級 | 新發現問題 | 來源／與前次 review 的關係 |
| --- | --- | --- | --- |
| F01 | P1 | Nullable live Lite group 使失敗的 `poll()` 取走 batch 並推進 delivered offset | `7e0e9955` 的 live-group 處理引入；相鄰於 R06／R08 |
| F02 | P1 | Consumer 獨立 inner-batch decoder 仍有固定限制，且未繼承訊息狀態 | R01 旁先前未覆蓋的路徑；外層 batch 修復未涵蓋 |
| F03 | P2 | POP orderly no-op 錯誤釋放 wait-ACK 計數 | `c0960129` 修正 R02 時引入 |
| F04 | P2 | 公開 compressor 仍拒絕超過 64 MiB 的合法解壓輸出 | R01 旁先前未覆蓋的公開 API |
| F05 | P2 | Lite 公開 group getter 未反映 Java 在 `start()` 的 namespace mutation | `7e0e9955` 新增 getter 後可觀察；相鄰於 R06／R08 的 start-state 缺口 |

P1 表示已確認的 delivery／offset 毀損風險；P2 表示可觀察的 API、狀態或 flow-control 差異。優先級不以 scheduler 時間完全相同或 Java 例外字串相同為依據。

## 2. 上一輪修復的重新驗證

| 前次項目 | 本輪複查結果 |
| --- | --- |
| R01，一般完整 record batch | 重跑更新後的原 Java／Rust probe：24 筆獨立 3 MiB 訊息與 65 MiB 完整 record 均已交付。剩餘 inner-container／公開 compressor 案例為 F02／F04。 |
| R02，額外的 POP orderly listener 工作 | 已有 no-listener／no-hook 分支，regression 通過；counter 狀態仍有 F03 差異。 |
| R03，detached listener task | POP 與 direct-consume callback 已使用 `AbortOnDrop`；POP cancellation regression 通過。 |
| R04，POP shutdown 等待倍增 | 已有 collective two-phase waiting；worker 數量不影響等待上限的 regression 通過。 |
| R05，Lite 初始 offset | 來源分支已區分 stored valid／missing／unavailable 與 retry topic；既有 focused tests 通過。另重跑原 Java probe。 |
| R06／R08，live settings 與 nullable groups | Setter、nullable construction 已存在，API／lifecycle tests 通過；新增 running-poll 與 namespace transition 測出 F01／F05。 |
| R07，retry delay | Configured／default／FLOW_CONTROL 分支與 paused-time tests 已存在並通過。 |
| R09／R10，TLS 指引與目前狀態 | README 已描述 chain／validity、不做 hostname check；current status 已區分歷史完整結果與三項修復版本 live replay。這些修正未新增確認問題。 |

`cargo test --workspace --locked` exit 0：94 筆 test-result 摘要，共 **2,052 passed、0 failed、0 ignored**，包含 doctests。其中包含目前錯誤斷言 counter 應為零的 POP orderly test；預期狀態錯誤的測試通過，不代表 Java parity。

[本輪 probe 結果](03-project-parity-rereview-evidence/results.json) 保留輸出、revision、source／probe hash 與驗證限制。[前次案例重跑結果](03-project-parity-rereview-evidence/previous-review-check.json) 記錄一般 batch 修復通過。備妥相依快取、repository Rust toolchain、Maven、JDK 後，可重現新案例：

```sh
python3 plan/2026/10/03-project-parity-rereview-evidence/run_probes.py --output /tmp/rocketmq-rereview-results.json
```

腳本建立連結目前實際 library 的暫存 crate，重用既有 Lite scripted-peer helpers 而不修改它們，並直接呼叫固定版本 Java 類別。Rust Lite 使用 loopback TCP peer 與受控 `OffsetStore`；Java null-poll 案例注入 running／cache state，以隔離該方法。未連線 live RocketMQ Broker／NameServer。POP counter 證據為 Java 實際呼叫，加上 Rust production source 與現有通過的 assertion；未新增執行公開 Rust POP-dispatch probe。本輪未重跑完整 Maven verify、Clippy、Docker／live scenarios 或跨平台檢查。腳本 assertion 描述目前缺陷，納入修正後 regression 時須更新。

## 3. 問題與改善要求

### 3.1 F01 — 失敗的 Lite poll 遺失 batch，並推進可提交的 offset

**位置：** [poll](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1113)、[取走 batch 與 delivered-offset 更新](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1923)、[commit_queues](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L1314)。Java：`impl/consumer/DefaultLitePullConsumerImpl.java:606` 的 `poll(long)`。

新增的 `poll()` 先呼叫 `take_ready_batch_with_queue()`，移除 batch 並推進 `delivered_offset`，才要求 `get_consumer_group()` 不可為 null。Running caller 現在可呼叫新增的 `set_consumer_group_nullable(None)`；即使沒有安裝 consume hook，`poll()` 也會在訊息已被移除後回傳 `InvalidConsumerGroup`。Java 接受 nullable setter，poll 只有建立 hook context 時才讀取 group，且該 context 本身也能表示 null。

**實際重現：** 啟動有 namespace、停用 auto-commit 的 Lite consumer，assign 一個 queue，script 回傳 queue offset 0 的合法訊息、next offset 1。把 live group 設為 null 後 poll，Rust 回傳 `Err(Pull(InvalidConsumerGroup))`。恢復 group 後以零 timeout poll，已沒有訊息。接著呼叫 `commit_queues([queue], true)`，受控 store 持久化 offset 1，應用程式卻未收到訊息。固定版本 Java 對已有 cached message、null group、無 hook 的 poll，回傳一筆訊息與 consume offset 1。

**影響：** Error result 隱藏了已被取走的 batch；explicit commit、後續 auto-commit 或 offset persistence 可能使遺漏持久化。已重現對受控 OffsetStore 的實際 commit，未宣稱驗證 live Broker persistence。

**改善：** Consume context 保留 Java 對 null 的接受能力，避免在 destructive dequeue 後執行依賴 group 的 fallible 工作。只在適用時建立 hook context，保留 nullable 狀態。檢查 dequeue 到成功交付之間的其他錯誤，避免 error 將 fetched work 靜默變成 delivered work。僅把相同錯誤 validation 移到 dequeue 前可避免遺失，但仍有公開行為差異。

**驗收：** 以實際 running `poll()` 測試 null／valid group mutation，分別涵蓋有／無 hook。Null 須如 Java 一樣交付訊息，hook group 保留 null。Error path 不得推進或持久化未交付 batch。保留正常 namespace reset、auto-commit 時點，以及 fetched／delivered offset 區別。外部重現案例須回傳一筆訊息，而非錯誤及空的 recovery poll。

### 3.2 F02 — 重複的 inner-batch decoder 仍改變合法交付與狀態

**位置：** [unwrap 呼叫與忽略失敗](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L184)、[consumer inner decoder](../../../crates/rocketmq-classic-consumer/src/pull/result.rs#L356)、[共用 Common 實作](../../../crates/rocketmq-classic-common/src/message/compact.rs#L181)。Java：`impl/consumer/PullAPIWrapper.java:77`；`common/org/apache/rocketmq/common/message/MessageDecoder.java:761` 的 `decodeMessage(MessageExt,List)`。

外層 decoder 已接受大型合法 batch，但 Consumer 仍保有獨立 compact parser，限制單筆 16 MiB、aggregate 64 MiB。Unwrap 失敗時，`process_pull_result` 保留原始外層訊息並回傳 FOUND。該私有 parser 建立新 `MessageExt` 時也未繼承外層 `WAIT` 設定。Common 已公開 Java 對應的 `decode_inner_batch`，依欄位長度解析、繼承 `is_wait_store_msg_ok`，並標示 client subtype。

**相同 encoded record 的實際比較：**

| 輸入 | Java PullAPIWrapper | Rust PullAPIWrapper |
| --- | --- | --- |
| 一個壓縮 inner container，包含 24 筆各 3 MiB compact body，同時設定 `INNER_BATCH_FLAG` 與 `NEED_UNWRAP_FLAG` | 24 筆邏輯訊息，next offset 24 | 一筆 packed outer message，next offset 24 |
| 兩筆小型 compact message，外層 `WAIT=false`，inner record 未指定 WAIT | 兩筆訊息，繼承 WAIT=false | 兩筆訊息，WAIT=true |

這些是 Java 可讀取的 offline records。本次未驗證未修改的 5.3.4 Broker 在預設設定下會產生大型 container；小型 inherited-state 案例不需要大型訊息。

**影響：** 應用程式收到 packed envelope，caller 卻保留超過 container 邏輯內容的 offset；inner tag filtering 也可能作用於錯誤 envelope。小型案例則在 unwrap 成功時仍改變公開訊息屬性。

**改善：** 重用 Common 的 Java-compatible inner decoder，對確實 malformed 的 compact data 保留 Java 的 all-or-original 行為。移除 consumer 專屬、拒絕合法 compact record 的限制，保留繼承的 WAIT／client-subtype 語意。可使用不同 Rust parser，但必須產生相同邏輯訊息與 inherited state，無須維護第二份分歧的相容實作。

**驗收：** 以公開 PullAPIWrapper 比較上述兩種輸入，再涵蓋單筆超過 16 MiB 的 compact body、ordinary／inner 混合、offset、tag、ID／client subtype、外層 WAIT true／false。Malformed-inner fallback 另保留原有覆蓋。擴充 Push／Lite 測試，驗證邏輯訊息交付與 committed offset，而不只檢查 outer decode 數量。

### 3.3 F03 — POP orderly no-op 釋放了未確認的工作

**位置：** [orderly early return](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5481)、[flow-control 讀取](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L5170)、[既有 regression 預期](../../../crates/rocketmq-classic-consumer/src/push/consumer.rs#L9257)。Java：`impl/consumer/ConsumeMessagePopOrderlyService.java:200,354`；`impl/consumer/DefaultMQPushConsumerImpl.java:524,564`。

修復已正確略過 orderly listener，但 return 前仍從 POP queue 的 wait-ACK count 減掉 `messages.len()`。Java 的 no-op request 沒有 acknowledge 或釋放此計數。Java 直接 submit、初始 count 1，結束時 listener 零次、count 仍為 1；Rust 現有通過的測試明確要求 count 0。原修復 probe 已記錄 Java count 1，與 Rust assertion 不一致。

**影響：** Running-info 狀態與 `popThresholdForQueue` admission 分支不同。Java 會累積 outstanding work，到 threshold 後阻擋下一次 POP；Rust 沒有發出 ACK 卻釋放額度，允許額外 POP／redelivery traffic。這是 state／control-flow 差異，不要求複製 Java executor。

**改善／驗收：** 固定版本 orderly no-op 路徑保留 outstanding count，同時保持零 listener／hook，以及零 listener-result ACK／visibility request。將錯誤 regression 預期改為 Java 結果。補 threshold 案例：初始 96，再增加一筆 outstanding message，在 threshold 96 下仍維持 97，使下一次 POP 不被允許。確認 concurrent consume、expiry handling、實際 ACK 路徑仍在對應生命週期事件釋放計數。

### 3.4 F04 — 公開 compressor 仍保留未公開的 64 MiB 上限

**位置：** [公開 Common decompression](../../../crates/rocketmq-classic-common/src/compression.rs#L33)、[公開 ProducerCompressor](../../../crates/rocketmq-classic-producer/src/compressor.rs#L31)。Java：`common/org/apache/rocketmq/common/compression/ZlibCompressor.java` 的 `decompress(byte[])`。

前次修正讓 message-record decoder 不受固定限制，但 `Producer::compressor().decompress()` 仍呼叫 `compression::decompress`，將輸出限制為 64 MiB。這是 Java compressor API 的公開對應，並非單純的私有 safety helper。

**實際重現：** 透過 producer 預設 compressor 壓縮 65 MiB，再解壓相同 bytes。Java 回傳 68,157,440 bytes，Rust 回傳 `InvalidData: decompressed message body exceeds configured limit`。因此公開 API 無法 round-trip 自己 compress 方法產生的 bytes。此獨立操作不受 Producer send-size validation 或 Broker 設定影響。

**改善／驗收：** 公開 compatibility decompression 支援 Java 接受的輸出範圍；只有預期的內部 call site 保留明確 bounded helper。不要新增 Rust-only 公開設定來取代缺少的 Java 能力。補 65 MiB 公開 round-trip 與固定 Java differential，再擴充支援的 Zlib／LZ4／Zstd 路徑及 malformed compressed data。保留 producer compression type 改變後，先前 compressor snapshot 的既有行為。

### 3.5 F05 — Lite group getter 未呈現 startup 的 namespace mutation

**位置：** [live group getter](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L286)、[startup namespace 計算](../../../crates/rocketmq-classic-consumer/src/lite/mod.rs#L735)。Java：`consumer/DefaultLitePullConsumer.java:233` 的 `start()`。

Java 將 `consumerGroup` 包上 namespace、寫回 facade，之後才呼叫 implementation startup。Rust 只計算 `effective_group` 供 validation，並準備有 namespace 的 Pull engine，未將結果寫回新的 live group field。公開 getter 狀態因此與 runtime group identity 不一致。

**實際重現：** Namespace `tenant`、group `invalid-thread-group`、pull thread count `-1`。Java 在本機 startup 以 `IllegalArgumentException` 失敗，但 getter 已是 `tenant%invalid-thread-group`；Rust 回傳 `InvalidPullThreadCount`，getter 仍是 `invalid-thread-group`。Scripted successful-start 案例也回報 raw `review-null-poll`，requests／store 卻使用有 namespace 的 group。差異是保留的 group 值，不是語言特有的 error class。

**改善／驗收：** 在 Java facade startup 對應時點，保留 namespace-wrapped group，維持 null／empty 與 namespace idempotence。Java 有區分 constructor-captured runtime ownership 與 facade mutable field 時，Rust 也保留該邊界；不要把後續任意 setter 輸入做成與 Java 不同的 normalization。測試 start 前、成功後、startup validation 失敗後、重複 start 的 getter，涵蓋無 namespace／已 wrapped／null／empty group，以及之後再次 setter。

## 4. 實作順序與驗證

1. 先修 F01，再做其他 Lite 變更；使用新增公開 nullable setter 補 end-to-end scripted poll／commit regression，同時驗證交付與持久化 offset。
2. F02 統一 inner decoder，F04 修正公開解壓；ordinary full-record、compact inner-record、public compressor 各自測試，避免一個邊界通過便掩蓋其他問題。
3. F03 修正 counter lifecycle 與錯誤既有預期；比較 Java no-op counter／flow-control 決策，保留 listener／cancellation／shutdown 修復。
4. F05 修正公開 startup mutation，與 F01 一起涵蓋失敗 start。所有驗收案例通過並記錄 revision 後，才更新 current status。

各變更執行 focused Common／Consumer／Producer tests 與固定 Java cases。完成後執行既有 gates：

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
mvn -o -f compat/java-reference/pom.xml verify
scripts/check-workspace-metadata.py
```

目前 2,052 項通過是 non-regression 基準。保留既有案例，修正已有 oracle 證明不一致的 assertion，新增實際可呼叫邊界測試，不只檢查 helper 回傳值或 getter／setter 是否存在。

環境相依驗證放在最後：重跑受影響的既有 local 5.3.4 Lite／Pull／POP scenarios，保留連結 revision 的輸出。Broker baseline 無法產生的 large-container／nullable-state 分支，以 scripted peers 覆蓋。只有實際需要正式環境的 owner-only scenarios 才由 owner 執行；這五項本機問題不依賴正式 credentials 或基礎設施。本次 review 不變更 CI 政策，也不宣稱已取得發布核准。
