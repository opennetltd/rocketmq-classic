# Plan 02 — 以「每請求自有任務」對齊 Java 的 Pull-RPC 併發扇出

## 1. 目標與範圍

把 Push consumer 中固定的 `PULL_RPC_WORKER_COUNT = 4` / `PULL_RPC_QUEUE_CAPACITY = 64` Pull-RPC worker 池，替換為**送件即返回、每個請求一個自有任務**的派發模型，使同時在途的 `PULL_MESSAGE` 數量像 Java 5.5.1 非同步回呼扇出那樣隨指派佇列數擴展。

範圍內：

- `crates/rocketmq-classic-consumer/src/push/pull_service.rs` 的 Pull-RPC 派發內部實作；
- `src/push/consumer/runtime.rs` 中一個送件呼叫點；
- 單元測試與一個新增的腳本地端（scripted-peer）整合測試；
- 相容性文件、一份 ADR 與執行日誌。

範圍外：POP worker 模型（已是每佇列自有任務）、consume-service 執行緒池、Phase 6 runtime 的 `PullMessageService` FIFO worker 及其無界佇列、公開 API/設定（不新增也不移除）、實際 Broker 場景（Phase 19）。

## 2. 背景脈絡

### 2.1 Java 5.5.1 參考行為（錨定原始碼）

| Java 元素 | 錨定事實 |
| --- | --- |
| `PullMessageService` | 一條 `ServiceThread` 加上**無界**的 `LinkedBlockingQueue<MessageRequest>`；延遲請求由單執行緒排程器（`PullMessageServiceScheduledThread`）重新塞回同一條 FIFO。 |
| `DefaultMQPushConsumerImpl.pullMessage(PullRequest)` | 結尾是 `pullAPIWrapper.pullKernelImpl(..., BROKER_SUSPEND_MAX_TIME_MILLIS = 20_000, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 30_000, CommunicationMode.ASYNC, pullCallback)`。派發執行緒立即返回，因此**每個指派佇列都能同時各持有一個在途長輪詢**。 |
| 回應處理 | `NettyRemotingAbstract.processResponseCommand` → `executeInvokeCallback` → client `getCallbackExecutor()`，即 `callbackExecutor ?: publicExecutor`；而 `publicExecutor = Executors.newFixedThreadPool(clientCallbackExecutorThreads)`，`ClientConfig.clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors()`。 |
| 設定項 | Java **沒有**任何 pull worker 數量設定。`consumeThreadMin/Max` 等屬於消費端，不是拉取端。 |

### 2.2 目前 Rust 行為（現行程式樹）

1. `PullScheduler` 只有一條自有到期任務（`run`，`src/push/pull_service.rs:581`），把到期請求交給 `ClientRuntime::execute_pull_request_immediately`（`crates/rocketmq-classic-core/src/runtime/runtime.rs:561`），其 FIFO 是無界 `VecDeque`（`crates/rocketmq-classic-core/src/runtime/pull.rs:55`）—這一段已經與 Java 一致。
2. `PushConsumerRegistration::pull_message`（`src/push/consumer/runtime.rs:111-125`）不會 await，只呼叫 `PullRpcWorkers::try_submit`。
3. `PullRpcWorkers` 是 **4 條任務加上有界 `mpsc(64)` 通道**（`src/push/pull_service.rs:107-190`）。每條任務會 await `PushConsumer::run_pull_worker_request` → `fetch_push_result` → `runtime.api().pull_message(...).await`，也就是整個長輪詢期間都佔用該任務（Broker 掛起 20 秒、client 期限 30 秒；常數在 `src/push/consumer.rs:235-236`，RPC await 在 `src/push/consumer/processing.rs:292`）。
4. 當 64 個槽位滿時 `try_submit` 失敗，請求會被以 `PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL = 50 ms` 重新排程（`src/push/consumer/runtime.rs:118-124`）。Java 因為佇列無界而沒有這條路徑。

對 `N` 個指派且大致空閒的佇列而言（每次輪詢都會掛到 Broker 20 秒掛起到期，然後以延遲 0 重新排程 — `src/push/consumer/processing.rs:366, 397`）：

| | 同時在途的 `PULL_MESSAGE` | 空閒佇列的首條訊息延遲 |
| --- | --- | --- |
| Java 5.5.1 | `N`（每個指派佇列一個） | ≈ 網路 RTT |
| 目前 Rust | `min(N, 4)` 在途 + 最多 64 個排隊 | ≈ `ceil(N / 4) × 20 秒`（16 佇列 → 約 80 秒、32 佇列 → 約 160 秒） |

### 2.3 差異的由來與其契約狀態

- 根本原因：Rust 在呼叫端任務內 await RPC。若直接在共用的 runtime FIFO worker 上 await，同一個 `ClientRuntime` 的所有 consumer 與佇列會被序列化；因此實作把 await 搬進私有執行緒池 — 但選了 Java 沒有的固定寬度。
- 造成此選擇的計畫壓力：主計畫 P10-04 —「Do not create an unbounded Tokio task for every pull iteration.」；Phase 10 計畫 §4.3 —「Use a bounded worker set and due-time queue rather than detached spawn per iteration.」
- 來源紀錄：commit `62253614 feat(consumer): wire bounded push pull pipeline`；唯一記載是 `rlogs/2026-09-11-phase-10-push-consumer-pipeline__20261011T135145Z.rlog:46` —「Added bounded pull RPC workers so broker long polls do not occupy the shared runtime pull-dispatch worker.」
- 目前契約狀態：**沒有 ADR、沒有 `KNOWN_DIFFERENCES.md` 條目**；相容性文件只把它當作證據文字提及（`docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md:56`、`docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md:844`）。主計畫契約写明 Tokio 原語「may replace JVM internals, not change observable semantics」、內部類別結構為「Not required」必須對照，且「no observable difference may be waived」。由於這個上限改變了可觀察量（同時在途的 pull 數量 → 多佇列下的送達延遲），此處視為應修正的相容性缺陷，而非已接受的框架差異。

### 2.4 本倉庫已有的先例

POP 路徑已經使用自有、可 join、可取消的每指派任務：`pop_workers: Mutex<HashMap<MessageQueue, JoinHandle<()>>>` 與 `pop_retired_workers`（`src/push/consumer.rs:665-666`）、rebalance 期間的生成/退役（`src/push/consumer/rebalance.rs:305-317, 470-485, 520-528`），以及關閉時透過 `shutdown_pop_worker_handles` 進行 abort/join（`src/push/consumer.rs:336-352`）。新的 Pull 派發沿用這個所有權模式，也符合主計畫「Implement these as cancellable tasks owned by [the owner], not detached Tokio tasks.」的要求。

Java / 目前 Rust / 目標的對照見 `diagrams/02-pull-dispatch-fanout.svg`。

## 3. 目標設計

- **送件即返回**：`try_submit` 登記請求識別碼並生成一條自有任務後立即返回，不做等待 — 這是 Java `CommunicationMode.ASYNC` 交接的 Rust 對應物。
- **扇出上限** = 在途識別碼數 ≤ 指派佇列世代數。這已由 `PullScheduler` 保證：每個 `PullIdentity {queue, generation}` 最多持有一個已排程/在途請求（`in_flight: HashSet<PullIdentity>`，`src/push/pull_service.rs:301-305`；由 `PullScheduler::finish`（第 512 行）釋放）。沒有固定 worker 數、沒有通道容量、也沒有送件退避延遲。
- **恰好一次的槽位釋放**由任務內建立的 RAII guard 負責：完成、取消、panic 或 abort 時都要 (a) 從 live map 移除自己的識別碼並 (b) 呼叫 `PullScheduler::finish`。guard 在 pull future 之後才解構，因此目前「先重新排程、再 finish」的順序（`src/push/pull_service.rs:170-178`）不變。
- **保留的生命週期**：`watch` 取消；`stop(wait_millis)` = 關閉送件 → 發出取消 → 在剩餘期限內 join 每條 live 任務 → abort 落後者並 await；`Drop` 會 abort。啟動取得順序、回滾路徑與 `StartupStage::PullRpcWorkersStarted`（含 `"pull-rpc-workers-started"` 字串）保持不變，因為這些名稱已出現在既有證據中。
- **已拒絕的替代方案**：每個指派 Pull 佇列一條長期迴圈任務（POP 式）。那會重複 Phase 6 `PullMessageService` 的所有權，並繞過 Java 單一服務執行緒所提供的 `PullScheduler` 延遲/合併/暫停/取消不變量。

## 4. 實作計畫

### 4.1 替換執行緒池內部 — `src/push/pull_service.rs`

- 刪除 `PULL_RPC_WORKER_COUNT` 與 `PULL_RPC_QUEUE_CAPACITY`（第 107-108 行），並移除不再使用的 `tokio::sync::mpsc` import。
- `PullRpcWorkerSet` 改為 `{ live: HashMap<PullIdentity, JoinHandle<()>>, cancellation: watch::Sender<bool> }`；保留 `PullRpcWorkers { state: Mutex<Option<PullRpcWorkerSet>> }`。送件關閉的條件就是 `state` 為 `None`。
- `start(consumer, scheduler)`：簽章不變，`AlreadyStarted` / `Stopped` 結果不變；只安裝狀態（不再生成迴圈）。把 set 包在 `Arc` 中，讓完成 guard 能在後續 `stop()` 取走之後仍然有效。
- `try_submit(request) -> Result<(), Arc<PullRequest>>`：在狀態鎖內 — 無狀態時返回 `Err(request)`；否則先清除已結束的 handle（`live.retain(|_, handle| !handle.is_finished())`），生成任務並插入 `(identity, handle)`，返回 `Ok(())`。不可跨 `await` 或跨 `PullScheduler` 呼叫持有狀態鎖。
- 任務主體：先 `upgrade` 那個 `Weak<PushConsumer>`（失敗時釋放槽位並結束）；安裝完成 guard；接著 `tokio::select! { biased; _ = cancellation.changed() => {}, _ = consumer.clone().run_pull_worker_request(request) => {} }`，維持現有「取消即釋放槽位」語意。
- 新增 `struct PullCompletionGuard`，其 `Drop` 先從 live map 移除自己的識別碼、再呼叫 `scheduler.finish(&request)`；其中不得含 await（只用 std mutex 與 `Notify`），以確保 abort 安全。
- `stop(wait_millis)` 與 `Drop`：演算法與今日相同，改為迭代 `live.values()` 而非固定向量。
- 僅供測試的存取器 `worker_count()` 更名為 `active_admissions() -> usize`（回傳 live 任務數），維持 `#[cfg(test)]`。

### 4.2 移除 Rust 專有的送件退避 — `src/push/consumer/runtime.rs:111-125`

當 `try_submit` 返回錯誤（關閉中或啟動回滾導致送件已關閉）時，改為呼叫 `self.pull_scheduler.finish(&request)` 並丟棄請求，而不是延遲 50 ms 重新排程。這對應 Java 中已停止的 `PullMessageService` 只記錄並丟棄（`executePullRequestLater` / `executePullRequestImmediately` 對 `isStopped()` 的防護）。保留 `PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL` 給 Java 對應的 ProcessQueue 快取流控（`src/push/consumer/processing.rs:134, 462`）。

### 4.3 呼叫點盤點（僅需可編譯）

確認以下位置仍可編譯且順序不變；預期簽章不需改變：`src/push/consumer.rs` 第 377、442、579/595/611、2562、2599、2618、2627-2630（啟動回滾）、2721-2728（start + checkpoint）、2740、3579-3609 、3728 與 3658（關閉）；`src/push/consumer/tests.rs` 有約 40 處 `Arc::new(PullRpcWorkers::new())` fixture（前幾處在第 457、623、961、1012、1806、2284、2491、2753、2848、2926 行），除非斷言改變否則不需修改；完成前用 `rg -n 'PullRpcWorkers::new|worker_count\(\)' crates/rocketmq-classic-consumer/src` 逐一盤點。

### 4.4 單元測試

- 在 `src/push/pull_service.rs`，以 `pull_dispatch_admits_every_distinct_identity_without_a_worker_cap_and_joins_on_stop` 取代 `pull_rpc_workers_have_a_fixed_owned_set_and_join_on_stop`（第 910 行）：提交超過 64 個不同識別碼並斷言每次 `try_submit` 都返回 `Ok`；第二次 `start` 仍返回 `AlreadyStarted`；`stop(100)` 後 `active_admissions() == 0`。
- 新增 `pull_dispatch_releases_the_scheduler_slot_when_a_pull_future_panics`：用會 panic 的 future 驅動完成 guard，並斷言 scheduler 的邏輯 pull 計數回到 0（不殘留 in-flight 識別碼）。
- 在 `src/push/consumer/tests.rs` 更新三處 worker 數斷言：`startup_registers_push_consumer_and_shutdown_releases_it`（第 8322 行斷言 `== 4`；改為「無固定上限」的斷言 — 啟動且尚無指派時 `active_admissions() == 0`，且不得是某個固定常數）、第 8381 行（關閉後 `== 0`，換用新存取器）、`shutdown_aborts_a_slow_listener_without_committing_its_batch`（第 9166 行）與 `shutdown_with_explicit_wait_overrides_configured_wait`（第 9275 行）維持「關閉後為 0」的斷言並換用新存取器。

### 4.5 新增腳本地端扇出測試 — `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs`

沿用 `crates/rocketmq-classic-consumer/tests/pull_consumer.rs` 的既有骨架（`TcpListener::bind("127.0.0.1:0")`、NameServer peer 回傳 route body、`read_command`、`frame::encode`、`respond_empty`）。Route fixture：單一 broker 帶 **16 個佇列**，走 Phase 11 的真實 rebalance 指派。

斷言：

1. 在 peer 尚未回應任何請求前，就已觀察到至少 16 個不同 `queueId` 同時在途於 `PULL_MESSAGE`，且每個佇列恰好一個請求 — 證明扇出等於指派數量。
2. 在同一佇列收到第一個回應之前，不會對同一 `queueId` 重複送出請求（目前會在這裡失敗：只到 4 個，其餘每 50 ms 重新進入）。
3. 全部回以 `NO_NEW_MSG` 之後，每個佇列都開始第二輪 pull。

### 4.6 證據與文件

- 新增 `docs/architecture/ADR-0085-push-pull-dispatch-fanout.md`：決策、§2.1 的 Java 原始碼事實、已拒絕的固定上限與每佇列迴圈替代方案，以及所有權/join 契約。
- 更新 `docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md` 第 56 行（把 "bounded RPC workers" 換成「扇出等於指派佇列數的每請求自有派發」）與第 63 行（"pull-RPC … workers" 措辭），以及 `docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md:844` 的證據欄。
- 在新的執行日誌 `rlogs/2026/11/02-push-pull-dispatch-fanout-parity__<UTC>.rlog` 記錄本次分析、變更與驗證。不得改寫歷史 Phase 10 計畫或 rlog 原文。

### 4.7 登記 Phase 19 live 場景 `P10-V05`

依負責人決定登記（見 §7）。live 場景清單在 `tests/differential/scenarios.yaml`（目前 37 筆；Push 的 `P10-V03` 在第 33 行），且每個場景 ID 另外硬寫在 `tests/differential/run.sh`。需登記處：

- manifest：在 `P10-V03` 那筆之後新增一筆，欄位為 `"id":"P10-V05"`、`"phase":10`、`"execution_phase":19`、`"profile":"basic"`（單一 Broker）、inputs `{"topic":"compat_p10_fanout","group":"compat_p10_push_fanout","queues":16,"messages_per_queue":1}`、assertions `["every-queue-first-delivery-inside-broker-suspend-window","java-and-rust-per-queue-first-delivery-latency-comparable"]`、該 topic 與 group 的 cleanup、`"normalization":["opaque","timestamps","message_id"]`，以及 `"live_status":"PENDING_PHASE_19"`。
- `tests/differential/run.sh` 需更新的 ID 清單：manifest 輸入驗證（第 148 行附近的 `by_id` 區塊與第 189-199 行的逐場景輸入防護）；第 343 行的 `implemented=(...)`；第 426 行的 topic 防護 `case "$scenario_id:$topic"`（加入 `P10-V05:compat_p10_fanout`）；第 546 行 `P10-V03:compat_p10_retry)` 旁邊的逐場景 profile/group 防護；第 625 行的長串場景 ID 排除鏈；以及逐場景命令腳本區塊（仿第 1074-1090 行的 `P11-V04` heredoc），其中必須用 `mqadmin updateTopic ... -r 16 -w 16` 建立 topic。第 412 行的 profile 白名單已接受 `basic:*`，第 638 行的雙 Broker 覆寫清單不需改動。
- 探針：在 `integration-tests/src/bin/live_client.rs` 新增 `p10-fanout <namesrv> <topic> <group> <ready-file>` 子命令（仿 `p10_push_probe`，第 135 與 453 行），為每個指派佇列記錄一筆 `(queueId, 首次送達時間)`；並在 `compat/java-reference/src/main/java/compat/` 新增對應 Java 探針及其在 `compat/java-reference/pom.xml` 的 `exec:java@...` id。
- 比對器：新增 `tests/differential/check_p10_fanout.py <evidence-dir> <topic>`，比較 Java 與 Rust 每個佇列的首次送達延遲。斷言必須容忍時間抖動（每個佇列都在 20 秒掛起視窗加固定容差內送達；任何佇列不得落後超過中位數的有界倍數），不可斷言精確毫秒值。
- 把新的比對器與探針路徑加入 `tests/differential/run.sh` 的 `code_dirty_paths` 清單（第 760-775 行），以維持 clean-source 計算正確。
- 文件：在 `docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md` 的 "Case ownership and status" 表格（第 213 行之後）新增 `P10-V05` 列，狀態 `PENDING_PHASE_19`，並在 `docs/compatibility/CURRENT_STATUS.md` 記錄新增的待執行場景。`docs/owner-validation.md` 中「37/37 live scenarios」等歷史計數保持原文不動。

## 5. 驗證方式

```sh
cargo fmt --all -- --check
cargo test -p rocketmq-classic-consumer push::pull_service::tests --locked
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test -p rocketmq-classic-consumer --all-targets --locked
cargo clippy -p rocketmq-classic-consumer --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

預期：既有 Push 生命週期/關閉測試（`startup_registers_push_consumer_and_shutdown_releases_it`、`shutdown_aborts_a_slow_listener_without_committing_its_batch`、`shutdown_with_explicit_wait_overrides_configured_wait`）不得退步，且新扇出測試通過。

live 閘門（已登記場景，於 Phase 19 執行）：`ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=basic tests/differential/run.sh --scenario P10-V05` 會在可丟棄的 5.3.4 Docker profile 下執行，`bash integration-tests/run-local.sh --all` 則把它併入總覽。該場景對一個靜默的 16 佇列 topic 每佇列發布一則訊息，並比較 Java 與 Rust 各佇列的首次送達延遲；每個佇列都必須在 Broker 掛起視窗內送達，而不是隨佇列數線性增長。該次執行完成前 `P10-V05` 維持 `PENDING_PHASE_19`；由於 `run.sh --all`/`--check-all` 遇到未實作的 ID 會 fail closed，§4.7 的登記工作必須與程式碼變更同一次落地。

## 6. 驗收標準

1. `PULL_RPC_WORKER_COUNT` 與 `PULL_RPC_QUEUE_CAPACITY` 不復存在，consumer crate 內不再殘留任何固定 Pull 併發常數。
2. 面對會 hold 住長輪詢的腳本 peer，`N` 個指派佇列會造成 `N` 個同時在途的 `PULL_MESSAGE`（每佇列一個）；50 ms 送件退避路徑消失。
3. 每個已提交的請求在完成、取消、panic 與 abort 時都恰好釋放一次其 scheduler 槽位（`active_admissions()` 與 scheduler 邏輯計數都回到 0）。
4. 關閉維持現有契約：關閉送件、在 `awaitTerminationMillisWhenShutdown` 內 join live 任務、abort 落後者，並如今日保留未處理的佇列內容與 offset。
5. 不新增、移除或改變任何公開 API、設定或預設值；Java 對應的延遲常數（`20 ms / 50 ms / 1 s / 3 s`）維持現有呼叫點，唯獨刪除送件退避那一處。
6. ADR-0085 加上 Phase 10 matrix 與上層相容性欄位都描述新的派發模型，且 rlog 補上先前缺失的平行性（parity）論證。
7. `P10-V05` 已存在於 `tests/differential/scenarios.yaml`、已接進 `tests/differential/run.sh` 每一處場景 ID 清單、兩種語言的探針與 `check_p10_fanout.py` 都已就緒，且在任何 live 執行記錄結果前維持 `PENDING_PHASE_19`。

## 7. 決策

- 決策：保留現有名稱 `PullRpcWorkers`、`pull_rpc_workers` 欄位與 `StartupStage::PullRpcWorkersStarted` / `"pull-rpc-workers-started"`。只改內部實作；改名會使相容性文件與歷史日誌已引用的字串失效。
- 決策：不對 live Pull 任務設固定上限。Java 沒有上限，扇出受指派數量限制，且每條任務都是自有、可 join。較大的固定上限（例如 32）已被拒絕，因為只是在另一個佇列數重現同樣的可觀察差異。
- 決策（負責人，2026-11-12）：登記一個永久 Phase 19 場景 `P10-V05`，不再只留負責人執行說明。這類缺陷在單元與腳本地端的時間斷言下看不出來，只有 live 長輪詢執行能證明扇出平行性；因此 §4.7 的登記工作屬於本計畫，而該次 live 執行本身仍是 Phase 19 義務，不阻擋程式碼變更合併。
