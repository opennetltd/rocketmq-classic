# Plan: Phase 11 — Rebalance 與 assignment lifecycle

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 11；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`12-phase-11-rebalance.md`](12-phase-11-rebalance.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

完成 public Push startup 與可重現 queue assignment，包含並行 pull/callback 下安全移除／新增 queue。

## 2. 前置條件與現況分析

Phase 10 private activation/removal 接點與 Phase 9 offset-boundary RPC 已備妥。orderly lock-specific integration 到 Phase 12 完成，目前不得宣稱 orderly ownership。

- 先完成: [Phase 10](11-phase-10-push-consumer-pipeline.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `RebalanceImpl`
- `RebalancePushImpl`
- `RebalanceService`
- `AllocateMessageQueueAveragely`
- `AllocateMessageQueueAveragelyByCircle`
- `consistent-hash and machine-room allocation classes`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-consumer/src/rebalance/mod.rs
crates/rocketmq-classic-consumer/src/rebalance/strategy/
crates/rocketmq-classic-consumer/src/rebalance/service.rs
crates/rocketmq-classic-consumer/src/push/consumer.rs
crates/rocketmq-classic-consumer/tests/rebalance.rs
examples/push_consumer.rs
```

每個 consumer 同時間只允許一次 rebalance 修改；wakeups 合併。strategy 前先以 Java 相容比較排序 MessageQueue 與 client ID。由一致 snapshot 建立 desired assignment，再與 current queue generation reconcile。consumer-list query 失敗不等於 authoritative empty group，不可因此任意重分配。ownership/offset 義務完成前不得移除 local state。

## 4. 依序實作步驟

### 4.1 P11-01 / P11-02 — 以 pure logic 實作 average allocation

使用主計畫 strategy signature；以 fixture 確認 invalid input/current-client-missing 的 error 或 empty result。精確移植 Java index/start/range 算術，包含 queue 少於 consumer。一起測全部 client：assignment 互斥且涵蓋 eligible queue，不只測 current client。

### 4.2 P11-03 — 盤點並實作全部 Java allocation strategy

盤點 pinned public allocation class。實作全部 Java public strategy，包含 circle 及 source 確認的 consistent-hash/machine-room variant，精確重現 hash input byte、virtual node、comparator 與 error。適當時重用已安裝 hashing dependency，不以 Rust DefaultHasher 代替。不可延後或排除任何 Java strategy；所有差異都阻擋完整 parity。

### 4.3 P11-04 / P11-05 — 解析 clustering 與 broadcasting assignment

broadcasting 分配全部 subscribed queue，使用 local offset。clustering 取得 consumer ID、排序後呼叫所選 strategy；依 fixture 區分 unavailable、empty 與 missing-current-client。更新 subscription snapshot；fetch 失敗於下一 owned cycle 重試。接上 startup 第 12 步，以 real rebalance 取代 Phase 10 test assignment。

### 4.4 P11-06 — 依 ownership 順序移除 assignment

取消 pending pull 前先標 dropped；停止新增 consume work，保留 unresolved offset 並 persist safe progress。cleanup 失敗需保留 removal state 供 retry，不可直接刪除證據。replacement generation 不得接收舊 listener/pull completion。現在安裝 orderly unlock 接點，Phase 12 啟用。cleanup 成功後才以 identity 移除 registry state。

### 4.5 P11-07 — 計算安全 initial offset 並啟用

只在 Java 定義的時點移除 stale in-memory offset，再讀 authoritative stored offset。不存在時，透過 Phase 9 RPC 實作各 ConsumeFromWhere policy，包含 Java fixture 中的 retry-topic 與 timestamp/time-zone 行為。無法取得 start offset 不可變成零。建立一個 ProcessQueue generation、啟用一個 PullRequest，activation 失敗則 rollback。Phase 12 會在 orderly activation 前插入 Broker locking。

### 4.6 P11-08 / P11-09 — 接上 trigger 並證明 race safety

管理單一 periodic service，支援 immediate startup、subscription-change 與 NOTIFY_CONSUMER_IDS_CHANGED wakeup。採 source 確認的 timing，以及 bounded/coalesced signal。shutdown 開始後拒絕新工作。以 barrier 測 remove-versus-pull、remove-versus-listener、concurrent-trigger 與 rapid join/leave；完成真正 public Push example 與 facade export。

### 4.7 P11-10 — 補齊 Pull consumer rebalance owner

移植 Java Pull consumer 的 allocation strategy 與 `enableRebalance` 預設值、queue-listener 註冊、topic/subscription 註冊，以及執行中的 in-balance 查詢。接入 runtime 管理的序列化 rebalance service：broadcasting 指派所有已訂閱 queue；clustering 使用 Broker membership，membership 無法取得時保留目前 assignment。於 subscription 與 query 邊界一致處理 namespace。移除 queue 前先 persist 安全 offset；移除失敗時保留狀態以供 retry；assignment 改變後通知 listener，shutdown 時拒絕舊 callback。此 Pull facade 範圍列為 P11-U02，並記入 Phase 11 compatibility matrix。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P11-V01** — Java/Rust allocation 在 0/1/N queue、1/N client、10q/3c、missing client、Unicode order 與全部 Java strategy 相同。
- [x] **P11-V02** — initial-offset policy 符合 Java；query/persist failure 不得重設為零或移除 live replacement。

- [x] **P11-U01** — Java allocation fixture、scripted membership change 與 barrier test 驗證 join/leave convergence、startup、retry、安全 offset 及 shutdown ownership。
- [x] **P11-U02** — 本機測試涵蓋 Pull rebalance 預設值、public listener/registration/query API、namespace mapping、broadcasting/clustering assignment、offset-safe removal，以及 membership 無法取得時保留 assignment。

### 5.1 實作完成後的本機命令

實作期間已執行下列 focused command。目前 package/workspace/Maven gate 結果記錄於[Phase 11 相容性矩陣](../../../docs/compatibility/PHASE_11_OFFLINE_METHOD_MATRIX.md)及較新的[Phase 11 延續執行紀錄](../../../rlogs/2026/09/12-phase-11-rebalance__20261012T011432Z.rlog)。

```bash
cargo test -p rocketmq-classic-consumer --test rebalance --locked
cargo build -p rocketmq-classic --examples --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P11-V03** — 本機 mixed Java/Rust group join/leave 在文件化 timeout 內收斂，無 persistent duplicate queue ownership；仍允許 transient redelivery。
- [ ] **P11-V04** — 真正 public Java producer→Rust Push 與 Rust producer→Java Push 通過 retry/offset 驗證；反覆 shutdown 不遺留 scheduler task。

## 6. 驗收與交接

完成 Phase 10–11 累積 public Push 門檻。orderly 項目維持 PARTIAL，直到 Phase 12 的 lock renewal、expiry 與 callback completion fencing 驗收。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 12](13-phase-12-orderly-consumption.zh-tw.md).
