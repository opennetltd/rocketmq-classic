# Plan: Phase 10 — Push consumer 核心 pipeline

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 10；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`11-phase-10-push-consumer-pipeline.md`](11-phase-10-push-consumer-pipeline.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

實作有界 pull scheduling、ProcessQueue 與 concurrent listener/retry 語意。multi-client assignment 刻意由緊接的 Phase 11 完成。

## 2. 前置條件與現況分析

Phase 7–9 已驗收。主計畫將 rebalance 排在 Push startup 後，形成依賴缺口。以 private assignment-activation 接點及 test-only fixed assignment 解決，不公開假的 production rebalance。完整 public Push startup 的門檻由 Phase 10–11 累計完成。

- 先完成: [Phase 9](10-phase-09-pull-offset-store.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `DefaultMQPushConsumerImpl`
- `PullMessageService`
- `PullRequest`
- `ProcessQueue`
- `ConsumeMessageConcurrentlyService`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-consumer/src/push/consumer.rs
crates/rocketmq-classic-consumer/src/push/pull_service.rs
crates/rocketmq-classic-consumer/src/process_queue.rs
crates/rocketmq-classic-consumer/src/consume/concurrent.rs
crates/rocketmq-classic-consumer/tests/push_pipeline.rs
docs/architecture/ADR-0003-consumer-process-queue.md
```

以 `BTreeMap<queue_offset, message>` 維持 local order，明確保存 cached count/bytes、dropped flag、assignment generation、last-pull time、consume state 與 lock field。in-flight listener batch 在 terminal consume outcome 前仍計入數量；duplicate pull 不得重複計數。pull/consume work item 帶 ProcessQueue generation。late work 不可修改 replacement queue，失去 ownership 後不可 commit。下一個 committed offset 為最低 unresolved offset；沒有 unresolved message 時，採 Java 相容 next offset。

## 4. 依序實作步驟

### 4.1 P10-01 / P10-02 — 驗證設定並實作可回復 startup

registration 前驗證 group/model/from-policy/subscription/concurrency/batch/listener。遵循主計畫 12 步 startup；現在建立 rebalance integration state，實際 allocation 於 Phase 11 啟用。選擇／載入 local 或 remote store、建立 consume service、register/start runtime、更新 route 與 heartbeat。每個 registration/task 都有 rollback；start 失敗不可暴露 Running handle。Phase 12 前，未支援 listener mode 必須明確拒絕。

### 4.2 P10-03 — 先實作 ProcessQueue operation

新增 insert/deduplicate、take-batch、complete/requeue 與 drop，使用短暫原子 state transition。offset 保持順序，共享 body 時 counter 仍須精確。呼叫 user code 前先取 snapshot；global 或同步 state lock 不可跨 callback。加入 scheduling 前先測 out-of-order completion、gap、duplicate insertion 與 dropped-generation completion；在 ADR-0003 記錄 ownership model。

### 4.3 P10-04 / P10-08 — 建立有界 scheduler

每個 queue/generation 只有一個 logical scheduled pull，支援 immediate/delayed wakeup、pause/resume 與 cancellation。使用有界 worker set 與 due-time queue，不可每輪 detached spawn。pull 前套用 Java count/MiB/span/backlog threshold；in-flight listener message 也計入 pressure。以 tracing counter 觀察 delayed pull 與 backlog，不記錄 message body。

### 4.4 P10-05 — 處理全部 pull outcome，避免跳過 message

FOUND 更新 next pull offset、插入 message，提交有界 consume work。NO_NEW/NO_MATCHED 只有不影響 unresolved message 時才套用 Java tag-offset correction。OFFSET_ILLEGAL drop generation，排程 corrected-offset persistence，透過 assignment 接點要求 removal；不可立即復活。late pull response 在 insert 前再驗 generation。transient pull failure 依 source 確認的 delay 重新排程。

### 4.5 P10-06 / P10-07 — 安全完成 concurrent batch 與 retry

明確建立 Java success/reconsume-later 與 ackIndex partial acknowledgement。clustering failure 使用 send-back；只有 Broker 接受 retry 責任後才移除 failed message，否則保留／本機 requeue。send-back 失敗時實作 Java fallback retry-message production，保留 origin ID、reconsume count、delay 與 max-reconsume property。broadcasting failure 依其獨立 Java 語意，不套 clustering retry。listener error/panic 不可當成功；隔離執行並保留 message。保留 hook context 接點，public hook 到 Phase 16 才加入。

### 4.6 P10-09 — shutdown 前先停止接收新工作

停止新增 pull/consume work、drop assignment、取消 long poll，並在有界時間內等待受管理 listener work。只 persist completed offset，保留 unresolved work 供 redelivery，再 unregister 並 join task。slow callback 不可阻塞 shared reader 或其他 queue。記錄 cancellation/panic policy，shutdown 不可宣稱 abandoned message 已成功處理。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P10-V01** — ProcessQueue duplicate/gap/out-of-order completion 證明 counter 正確，且不 commit 越過 unresolved offset。
- [x] **P10-V02** — paused-time scheduler 證明 task 有界、threshold 正確、拒絕 dropped generation，send-back 失敗仍保留 retry。
- [x] **P10-V04** — 每個 startup step 的 failure injection 及 slow callback shutdown，不丟失 unacknowledged work，也無 owned-task leak。

- [x] **P10-U01** — private fixed-assignment test 注入 Java message fixture 與 scripted send-back response，驗證 listener success/partial/failure、retry 及 DLQ decision。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-consumer --test push_pipeline --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P10-V03** — test-only fixed assignment 消費 Java 產生的 message，並以本機 Broker 測 listener success/partial/failure、retry count 與 configured DLQ transition。

## 6. 驗收與交接

pipeline 可在 public Push 標為 PARTIAL 下合併；必須接續完成 Phase 11，才可宣稱正常 Push startup 或 mixed-group 相容。fixed assignment 僅限 private test。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 11](12-phase-11-rebalance.zh-tw.md).
