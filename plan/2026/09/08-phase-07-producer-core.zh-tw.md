# Plan: Phase 7 — Producer 核心

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 7；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`08-phase-07-producer-core.md`](08-phase-07-producer-core.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

提供 sync/future/oneway/batch send，包含 Java 相容 validation、queue selection、compression 與 retry budget。transaction 與 request/reply 留給後續 phase。

## 2. 前置條件與現況分析

Phase 0–6 已驗收。現在新增 producer 與薄 facade crate。namespace/security/trace parity 由 Phase 16 提供：只通過本階段，尚未滿足主計畫 v0.1 的 namespace/security 發布宣稱。

- 先完成: [Phase 6](07-phase-06-runtime-routing-heartbeat.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `DefaultMQProducer`
- `DefaultMQProducerImpl`
- `Validators`
- `MessageClientIDSetter`
- `MessageBatch`
- `TopicPublishInfo`
- `MQFaultStrategy`
- `latency fault classes`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-producer/Cargo.toml
crates/rocketmq-classic-producer/src/producer.rs
crates/rocketmq-classic-producer/src/config.rs
crates/rocketmq-classic-producer/src/send.rs
crates/rocketmq-classic-producer/src/selector.rs
crates/rocketmq-classic-producer/src/fault.rs
crates/rocketmq-classic-producer/src/batch.rs
crates/rocketmq-classic-common/src/compression/
crates/rocketmq-classic/Cargo.toml
crates/rocketmq-classic/src/lib.rs
crates/rocketmq-classic-producer/tests/producer.rs
examples/producer.rs
```

集中預設值：send timeout 3000 ms、compression threshold 4096 bytes、sync retries=2、async retries=2、retryAnotherBrokerWhenNotStoreOK=false、max message size=4 MiB、default queue count=4。由 public entry 到 routing/compression/attempt 共用一個 deadline。Rust send().await 對應 Java 等待結果的 send 語意；若 Java async-mode retry policy 不同，另提供明確命名且回 future 的 async-mode method。不可因兩者都是 Rust async 而靜默混用。

## 4. 依序實作步驟

### 4.1 P7-01 / P7-02 — 建立 lifecycle，I/O 前先驗證輸入

建立 validated config 與明確 start/shutdown，向 Phase 6 runtime 註冊並在 start 失敗時 rollback。新增 facade re-export 與可執行 example，不暴露 internal crate。在 facade Cargo.toml 以 `[[example]]` path 明確註冊 root `examples/*.rs`，確保 `cargo build -p rocketmq-classic --examples` 實際建置它們；後續 example 沿用此慣例。由 Java 移植 topic/body/property/system-topic validator，測 empty body 與 missing body 差異及 encoded size 邊界。重試期間 user Message 維持 immutable。

### 4.2 P7-03 / P7-04 — 取得 route 並選擇 queue

先用 cached publish info，再 refresh；只有 pinned client 使用時才套用 Java default-topic fallback/queue-count 行為。沒有 writable queue 時回 typed no-route error。實作 overflow-safe round-robin 與避開上次失敗 broker，再加入 explicit queue 與 custom selector path 並驗證 membership。selector 順序與 retry 行為需專用 Java fixture。

### 4.3 P7-05 / P7-06 / P7-07 — 建立單一 immutable send payload

加入 Java 相容 unique-ID 格式與 fixture，不用 random UUID 代替。建構 header/property/sysFlag，符合條件的 retry 保留原 timestamp/ID。依 source 確認的 threshold 規則與 flag/backend 壓縮；compressed bytes 非決定性時比對解壓後語意。保留原 body 供 hook 使用，compressed bytes 只建立一次，避免反覆壓縮。依 Java 選擇 V2。

### 4.4 P7-08 / P7-09 / P7-11 — 實作各 mode 的 attempt 與 result

先從 Java exception/status 擷取 pure retry-decision table 並測試。sync 總次數為 1+retries；async 與 oneway 遵循各自 Java policy。每次 attempt 後更新 fault state，保留 Broker code/remark，且只使用剩餘 deadline。Java 回 SendResult 的 storage-status response 不可自動轉成 exception。記錄 timeout-after-store 的重複投遞風險，不保證 exactly-once delivery。

### 4.5 P7-10 — 驗證與編碼 batch

以 Java 反例 fixture 記錄 5.5.1 batch-validation 變更：empty batch、mixed topic、source 確認不支援的 delay/retry/transaction 形式、不一致 property 與 encoded total-size limit。network I/O 前拒絕。依 Java batch body layout 編碼，不可串接 single-message Remoting command；驗證總長與 element length。在插入 property/ID 後測 size 邊界。

### 4.6 P7-12 — 基本 send 通過後再加入 latency fault 行為

由 pinned source 移植 latency isolation table、unavailable-broker fallback 與 optional detector behavior，pure test 使用可注入 clock。detector task 由 Producer shutdown 管理。以 two-Broker profile 測首個 attempt failure 與 route change；不以新 circuit-breaker library 代替相容行為。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P7-V01** — 預設值與 validation 邊界符合 Java，包含 threshold−1/threshold/threshold+1 與 max-size ±1。

- [x] **P7-U01** — scripted 雙位址失敗與 Java decision fixture 驗證 retry、status、deadline、body/ID 保留及反覆 lifecycle cleanup。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-producer --locked
cargo build -p rocketmq-classic --examples --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P7-V02** — sync、async-mode、oneway、explicit queue、selector 與 valid/invalid batch 情境通過 Java differential。
- [ ] **P7-V03** — two-Broker failure 證明 attempt count、status classification、deadline exhaustion，以及原 body/unique ID 未被改動。
- [ ] **P7-V04** — 持續本機 send 與反覆 start/shutdown 後，pending request、connection 與 producer-owned task 為零。

## 6. 驗收與交接

Producer compatibility 是進入進階 Consumer 工作的門檻。據實記錄延後的 namespace/hook exposure；Phase 8 重用 common compression registry，不另建一套。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 8](09-phase-08-message-decode-pull-results.zh-tw.md).
