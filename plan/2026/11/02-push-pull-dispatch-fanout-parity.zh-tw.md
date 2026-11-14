# 計畫 02 — 以每請求自有的 Push 派發對齊 Java Pull-RPC 扇出

## 1. 目標與範圍

將 Push consumer 固定的 `PULL_RPC_WORKER_COUNT = 4` / `PULL_RPC_QUEUE_CAPACITY = 64` 執行池，替換為送件即返回的自有任務派發。在穩定指派 `N` 個符合拉取條件的 Pull 佇列、且 transport 許可充足時，全部 `N` 個佇列都必須能同時發出長輪詢，與 Java 5.5.1 一致。

範圍內：`crates/rocketmq-classic-consumer/src/push/pull_service.rs` 的 Pull 派發、`src/push/consumer/runtime.rs` 的送件呼叫點、既有 async Pull API 選擇與私有 Pull 掛起常數、生命週期/回歸測試、一個腳本 peer 整合測試、文件/ADR/rlog，以及永久 Phase 19 場景 `P10-V05` 的完整 runner。下文以 `src/push/` 開頭的路徑均相對於 consumer crate。

範圍外：POP 派發與輪詢行為、consume-service 執行池、Phase 6 runtime FIFO 所有權、公開 API/設定、transport 送件政策，以及實作期間執行 live Broker 場景。§4.2 明確納入小幅掛起常數修正；不得改變 POP 對共用常數的使用。

## 2. 現況分析

### 2.1 Java 5.5.1 參考行為

參考 commit：`efa1be59d6fc747ad220f036c86030f05f0419f9`。

| 元素 | 已確認行為 |
| --- | --- |
| `PullMessageService` | 一條服務執行緒從無界 `LinkedBlockingQueue<MessageRequest>` 取件；單執行緒排程器重新送入延遲請求。 |
| `DefaultMQPushConsumerImpl.pullMessage` | 使用 `CommunicationMode.ASYNC`、**15,000 ms** Broker 掛起與 **30,000 ms** client timeout。長輪詢期間不會持續佔用派發執行緒。 |
| Transport 限制 | `NettyRemotingAbstract.invokeImpl` 取得 `semaphoreAsync`；`NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE` 預設 **65,535**，可透過 `com.rocketmq.remoting.clientAsyncSemaphoreValue` 設定。Java 沒有額外的四 worker Pull 上限，但 async RPC 總併發並非無限。建立連線與取得許可都可能延遲交接。 |
| 回應處理 | 通常使用 callback executor 或 `publicExecutor`；callback 執行緒數不等於在途長輪詢數。預設 callback 執行緒設定在 `NettyClientConfig`；`ClientConfig` 可設定它。 |
| 已停止的服務 | `executePullRequestLater` 檢查 `isStopped()`。`executePullRequestImmediately` 直接放入 FIFO，沒有停止防護。不得用不存在的 Java 防護來解釋 Rust 關閉行為。 |

錨定原始碼：[PullMessageService](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/client/src/main/java/org/apache/rocketmq/client/impl/consumer/PullMessageService.java)、[DefaultMQPushConsumerImpl](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java)、[NettyRemotingAbstract](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyRemotingAbstract.java)、[NettySystemConfig](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettySystemConfig.java)、[NettyClientConfig](https://github.com/apache/rocketmq/blob/efa1be59d6fc747ad220f036c86030f05f0419f9/remoting/src/main/java/org/apache/rocketmq/remoting/netty/NettyClientConfig.java)。

### 2.2 目前 Rust 行為

1. `PullScheduler::{run,take_due_inner}` 將到期請求交給 `ClientRuntime::execute_pull_request_immediately`。core runtime FIFO 使用無界 `VecDeque`；此階段不需變更。
2. `PushConsumerRegistration::pull_message` 立即呼叫 `PullRpcWorkers::try_submit`；不在共用 runtime worker 上 await RPC。
3. `PullRpcWorkers` 擁有四條任務與 `mpsc(64)`。每個 worker 在整段 RPC 期間 await `PushConsumer::run_pull_worker_request`。目前 Rust Pull header 使用 **20,000 ms** Broker 掛起；client timeout 為 30,000 ms。
4. 通道溢位後以 50 ms 快取流控延遲重新排程。只有容量耗盡時才發生，並非只要 `N > 4` 就會發生；16 個佇列不會填滿通道。
5. `fetch_push_result` 目前呼叫 `MQClientApi::pull_message`，採同步等待/socket-timeout 語意。既有 `pull_message_future` 對應 Java ASYNC Pull，包含清理寬限期（見 `docs/architecture/ADR-0003-typed-rpc-invocation-modes.md`）。Push 應選用此既有路徑；同步 Pull API 呼叫端不變。
6. remoting crate 已有可設定的 async 送件控制（`client/config.rs`、`client/invocation.rs`）與取消清理（`client/pending.rs::PendingGuard`）。保留這些保護。

對穩定的空佇列而言，若每次輪詢都等待完整掛起視窗 `S`，最後一個佇列的**初始請求**可能等待約 `(ceil(N / 4) - 1) × S`：以目前 Rust `S = 20 s` 計算，16 個佇列約 60 s、32 個約 140 s，不含初始化與排程。這是說明用估算，不是保證的首條訊息延遲。發布訊息可能立即完成前面的輪詢並釋放 worker，因此對全部佇列發布訊息、只測送達時間，可能漏掉缺陷。Java 在許可充足時，能為每個符合條件的佇列維持一個在途輪詢。

### 2.3 契約與所有權

Phase 10 選用有界執行池的紀錄位於 commit `62253614` 與 `rlogs/2026/09/11-phase-10-push-consumer-pipeline__20261011T135145Z.rlog`。主計畫禁止無界 detached task 的限制，並不要求固定四個 RPC 上限：透過 scheduler 識別碼送件的自有任務符合所有權要求。保留歷史計畫/日誌原文，以文件記錄此取代原設計的決策。

`PullScheduler` 以 `PullIdentity { queue, generation }` 合併工作。執行中的 pull 還可保留一個 deferred 後續請求，由 `finish` 釋放。rebalance 取消會標記舊世代，但不會立即終止其 RPC。因此精確上限是每個已送件的佇列世代最多一個 active 請求，**包含退役中的世代**，而非 rebalance 期間嚴格受目前指派數限制。`logical_pull_count()` 排除已取消的識別碼，scheduler 關閉時也會清空；單憑它為零，無法證明任務已 join。

POP 路徑提供所有權先例，但任務不得自行移除/丟棄尚未完成的 `JoinHandle`：那會使執行尾端脫離管理，關閉時也看不到它。只在 async body 內建立 completion guard，則會漏掉首次 poll 前的 abort。以下目標設計避開這兩個問題。

見[派發對照圖](diagrams/02-pull-dispatch-fanout.svg)。

## 3. 目標設計

- 保留 scheduler 與 runtime FIFO。`try_submit` 同步登記一條任務後返回；不額外加入固定 Pull 併發/通道上限。transport 許可、快取流控、暫停及過期指派檢查仍具效力。
- 使用自有 `tokio::task::JoinSet<()>` 管理任務 handle。送件時回收已完成結果，停止時全部 drain；任務本身不擁有也不修改 handle 集合。現有 Tokio 相依已足夠。已完成的項目可保留到下次送件或停止，但不得隨 pull 總迭代次數累積。
- **spawn 前**建立 `PullCompletionGuard`，移入 future，涵蓋整段 pull/取消作用域。它只擁有 request、scheduler 與獨立 active 計數器。在 pull future 已被丟棄後，呼叫一次 `scheduler.finish`；涵蓋 `Weak` 升級失敗、panic 展開、取消，以及首次 poll 前的 abort。guard 不取得 dispatcher 狀態鎖，也不保留強引用的 `PushConsumer`/dispatcher。
- 保留先重新排程、再 finish 的順序：`run_pull_worker_request` 可記錄 deferred 後續請求；只有 guard 釋放 scheduler 槽位。下一條任務送件時，已完成的上一條任務可能仍等待 join；由於前一條 pull future 已結束，這是安全的。
- 停止時原子關閉送件、發出取消，再於既有局部等待預算內 join。期限到達後 abort 全部剩餘任務並 drain join 結果，讓取消清理完成。`Drop` 透過自有集合 abort；runtime poll 到 abort 後才可觀察到非同步清理。不得宣稱同步 `Drop` 會 join，或 abort 清理具有嚴格實際時間上限。
- 保留啟動/關閉取得順序、回滾、`PullRpcWorkers`、`pull_rpc_workers`，以及 `StartupStage::PullRpcWorkersStarted` / `"pull-rpc-workers-started"`。不需每佇列永久迴圈，否則會重複 scheduler 所有權。

## 4. 實作計畫

### 4.1 替換執行池內部 — `src/push/pull_service.rs`

1. 移除 `PULL_RPC_WORKER_COUNT`、`PULL_RPC_QUEUE_CAPACITY` 與不再使用的 `mpsc` import。保留 `PullRpcWorkers { state: Mutex<Option<PullRpcWorkerSet>> }`。
2. `PullRpcWorkerSet` 儲存 `Weak<PushConsumer>`、`Arc<PullScheduler>`、watch sender、`JoinSet<()>` 與 `Arc<AtomicUsize>` active 計數器。不需把整個 set 包進 `Arc`。`None` 表示送件已關閉。
3. `start(consumer, scheduler)` 安裝空集合，不預先生成 worker。集合存在時仍返回 `AlreadyStarted`。目前 dispatcher 在 `stop` 後**不會**返回 `Stopped`；不要誤套 scheduler 不同的重啟契約。
4. 在狀態鎖內，`try_submit` 拒絕不存在的狀態、以 `try_join_next` drain 可取的結果（回報非預期 panic）、複製 cancellation/consumer/scheduler/counter、增加 active 計數、建立 guard，並在解鎖前 spawn 進集合。正式請求仍必須透過 `PullScheduler` 進入；不得繞過識別碼送件控制。持有 dispatcher 鎖時不得 await 或呼叫 scheduler 方法。
5. future 在任何 await 或 `Weak` 升級前，就擁有預先建立的 guard。進入 `changed()` 與 `run_pull_worker_request` 的 biased `select!` 前，先檢查 cancellation receiver 目前值。取消後才首次 poll 的任務不得發出新 RPC。透過作用域確保先丟棄 pull future，再丟棄 guard。guard 解構時減少 active 計數，再呼叫 `scheduler.finish`，不存取 dispatcher 狀態。
6. `stop(wait_millis)` 在鎖內取走 set、解鎖、發出取消，並在單一期限內反覆 `join_next`。處理成功、取消與 panic 結果，不得遺棄其他任務。到期時 `abort_all`，然後持續 drain `join_next` 直到空集合。`Drop` 發出取消，讓自有 `JoinSet` abort 在途任務。不得引入 set 與 guard 互持的引用循環；set 以弱引用儲存 consumer，只在執行時升級。
7. 只把 dispatcher 的測試存取器 `worker_count()` 改為 `active_admissions()`。以獨立計數器讀取 active 請求數，不可用 `JoinSet::len()`（包含已完成但尚未 join 的任務）。保留 `PullScheduler::worker_count()`。測試須保留 counter/set 的清理觀察依據，跨越 `stop` 取走狀態的動作；僅因狀態為 `None` 而返回零的存取器，不能證明已清理。

### 4.2 對齊交接、async RPC 選擇與私有 Pull 掛起常數

- `src/push/consumer/runtime.rs::pull_message` 在送件已關閉時，呼叫 `self.pull_scheduler.finish(&request)` 並丟棄請求；只移除溢位重試。這是 Rust 生命週期清理，不代表 Java 的立即入列具有停止防護。保留 50 ms 常數及真正的 ProcessQueue 快取流控用途。
- 在 `src/push/consumer/processing.rs::fetch_push_result` 選用 `runtime.api().pull_message_future`，取代 `pull_message`。保留絕對 30 s 請求期限；使用既有 async transport 清理寬限期與失敗映射，不再沿用同步逾時關閉 socket 的行為。參考 core `rpc_modes` 測試新增暫停時間/腳本 peer 模式回歸測試。不得改變 core API 或其他呼叫端。
- 在 `src/push/consumer.rs` 新增 `PULL_BROKER_SUSPEND_MAX_TIME_MILLIS = 15_000`，並在 `src/push/consumer/processing.rs` 的 Pull header 使用它。既有 20,000 ms 常數保留給 `run_pop_worker`，不得改變；目前 Pull 與 POP 共用此常數。Pull client timeout 維持 30,000 ms。這能修正獨立確認的私有 Pull wire 差異，又不改變 POP 或公開設定。

### 4.3 盤點生命週期與呼叫點

檢查 `PushConsumer::{start,start_registered,shutdown_with_await_termination_millis}`、`start` 的錯誤/回滾分支與測試 fixture；保留既有 scheduler-stop → dispatcher-stop 順序。預期不需改簽章。使用：

```sh
rg -n 'PullRpcWorkers::new|pull_rpc_workers|worker_count\(\)|BROKER_SUSPEND_MAX_TIME_MILLIS' crates/rocketmq-classic-consumer/src
```

只改 dispatcher 數量斷言；consume-service 與 scheduler 計數仍有意義。更新描述有界 RPC 執行池的註解。不要機械式更名所有 `worker_count()`。

### 4.4 單元與生命週期測試

將自有任務包裝抽成可搭配受控 future 的私有 helper；測試必須走與正式環境相同的 guard/spawn/join 路徑，不得公開測試 API。

- 替換 `pull_rpc_workers_have_a_fixed_owned_set_and_join_on_stop`：以 `schedule`/`take_due` 送入 **80 個不同、經 scheduler 接納的識別碼**，用 barrier hold 住其任務 future，證明全部 80 條可同時進入。對已失效的 `Weak` 送件並只檢查 `Ok`，不能測出併發能力。斷言第二次 `start` 為 `AlreadyStarted`，再取消/join，檢查保留下來的清理觀察依據。
- 涵蓋帶 deferred 重新排程的正常完成、失效 `Weak`、panic、watch 取消、對已 poll 且 pending 的 future abort、**首次 poll 前 abort**，以及 dispatcher `Drop`。沒有 deferred 工作的完成路徑，須在 scheduler 仍執行時驗證原始 `in_flight` 集合與 active 計數器歸零；觀察 panic/取消的 join 結果。有 deferred 工作時，驗證恰好一個後續請求到期。
- 測試送件與停止競爭、零等待預算：每個請求不是被擁有並 drain，就是被拒絕並由呼叫端 finish。檢查首次 poll 前已發出取消，以及重複 stop。abort 測試必須等待 join 或解構觀察依據；不可由 `state == None` 或 `scheduler.stop()` 清空計數推論成功。
- 驗證相同識別碼快速後續請求，以及不同世代的取消/重新指派；舊請求完成不得釋放新世代槽位。反覆多輪後，證明已完成任務項目會回收，不會每次迭代都保留。
- 在 `src/push/consumer/tests.rs`，`startup_registers_push_consumer_and_shutdown_releases_it` 於尚未指派前應期待零個 active admission，取代四個閒置 worker。更名其關閉斷言，以及 `shutdown_aborts_a_slow_listener_without_committing_its_batch`、`shutdown_with_explicit_wait_overrides_configured_wait` 的對應斷言；保留未解決訊息/offset 與回滾檢查。原 scheduler/consume-worker 斷言不變。
- 在腳本請求測試斷言 Pull `suspendTimeoutMillis == 15000` 與 30 s deadline；保留 POP `pollTime == 20000` 的斷言。

### 4.5 腳本 peer 整合測試 — `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs`

沿用 `tests/pull_consumer.rs` 的 frame/route helper 與 `src/push/consumer/tests.rs` 的 Push 啟動 helper。一個 broker 提供 16 個讀/寫佇列；單一 clustering Push consumer 經真實 rebalance 取得全部 16 個目標 topic 佇列。處理 route 查詢、heartbeat、consumer-list 查詢、offset 查詢/初始 offset 查詢、client 設定檢查與 unregister；分開過濾 retry-topic pull。不得在等待 hold 住的 Pull 回應時阻塞 peer 讀取。跨連線以 `(connection, opaque)` 及可取得的佇列世代追蹤請求。

1. 回應初始化/控制請求，但 **hold 住全部目標 topic Pull 回應**。在單一短測試期限內（例如 5 s），要求佇列 `0..15` 各恰好一個在途請求，且此前未回應任何目標 Pull。舊四 worker 執行池會卡在此 barrier；不需等待 20 s 或估算送達延遲。對 16 個佇列，舊實作不預期溢位或 50 ms 重試。
2. 回傳 `response::PULL_NOT_FOUND`（解碼為 `PullStatus::NoNewMessage`），帶有效的 `suggestWhichBrokerId`、`nextBeginOffset`、`minOffset`、`maxOffset` header。空的成功回覆不是有效 Pull 回覆。檢查 15 s 掛起 header。
3. hold 住第二輪，再次要求每個目標佇列各一個在途請求。除初始扇出外，也證明槽位釋放與重新排程。
4. 輪詢仍開啟時關閉；確認 consumer shutdown 與全部 peer 任務完成，沒有繼續送件或未結束的測試任務。使用有界等待及明確 peer teardown。

### 4.6 證據與文件

- 若編號仍可用，新增 `docs/architecture/ADR-0085-push-pull-dispatch-fanout.md`；否則選下一個未使用 ADR 編號。解釋取代 Phase 10 設計的所有權決策、transport 限制條件、15 s Pull/20 s POP 區別，以及 join/guard 契約。
- 更新 `docs/compatibility/PHASE_10_OFFLINE_METHOD_MATRIX.md` 的 Pull 與 shutdown 列、`docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md` 的 Phase 10 證據，以及 `docs/compatibility/CURRENT_STATUS.md`。分開記錄本地證據與待執行 live 結果；不得提升整個 phase 狀態或改寫歷史驗證計數。
- 在 `rlogs/2026/11/02-push-pull-dispatch-fanout-parity__<UTC>.rlog` 記錄實作與驗證。更新共用 SVG，使上限條件與修正後事實一致。

### 4.7 完成永久 Phase 19 場景 `P10-V05`

保留已記錄的場景登記決策。腳本 peer 已能證明併發；live 案例補上真實 Broker/client 證據。只登記 metadata 或命令紀錄並不足夠，因為 `run.sh --all` / `--check-all` 遇到不支援 ID 會 fail closed。

**Manifest 與環境**

- 在 `tests/differential/scenarios.yaml` 新增 `P10-V05`，包含 `source_case_id: P10-V05`、phase 10、execution phase 19、**profile `host-routed`**、inputs `topic: compat_p10_fanout`、`group: compat_p10_push_fanout`、`queues: 16`、`messages_per_queue: 1`、`admission_window_ms: 5000`、`delivery_window_ms: 5000`。在 host 執行的 Java/Rust Push client 必須取得可連線的 route 位址；`basic` Broker 宣告的是僅 Docker 內可用的 `broker`。
- 使用分開的 `${topic}_java` / `${topic}_rust` topic 與 `${group}_java` / `${group}_rust` group。每次只執行一個 consumer、採真實 rebalance，不使用固定分配策略。在 `broker-host` 以 `-r 16 -w 16` 建立各 topic，並建立兩個訂閱群組（自動建立已停用）。依既有 Push 初始化/清理模式管理場景 retry topic。
- 斷言：完整目標 topic 指派、任何目標 Pull 回應之前已開始全部 16 個空佇列 pull、每佇列一個 active pull、每個已發布訊息的佇列皆在宣告期限內送達，以及記錄 Java/Rust 各佇列經過時間。包含兩種 client 方向、完整衍生 topic/group 清理，以及 `live_status: PENDING_PHASE_19`。

**探針與量測**

- 在 `integration-tests/src/bin/live_client.rs` 新增 Rust `p10-fanout <namesrv> <topic> <group> <ready-file>`；在 `compat/java-reference/src/main/java/compat/` 新增 Java `LivePullFanoutProbe` 與對應 Maven exec id。使用立即確認成功的 concurrent listener；新 topic 的初始 offset 一致設定為 `ConsumeFromFirstOffset`。
- 透過既有 `RpcHook::{before_request,after_response}` / Java `RPCHook` 觀察 Pull 開始/回應，依精確 topic 與 group 過濾。記錄指派鍵值、Pull 事件與請求掛起時間。維護 active 佇列計數；重複的 active start 應失敗。hook 觀察的是呼叫邊界，不是實際封包到達；wire 層證據由腳本 peer 測試提供。transport 許可須高於測試需求。
- 從空 topic 開始。另給指派一段有界啟動期間。從第一個目標 Pull 呼叫起，要求 **5 s 內、且任何目標 Pull 回應之前**，全部 16 個不同佇列都已有 active pull；到此才寫入 ready file。即使所有訊息原本都能快速消費，四 worker 實作仍會在此閘門失敗。不得提前發布訊息，或以無限期的穩定等待取代閘門。
- 閘門通過後，由同一探針程序內可重用的 producer，明確對每個已指派 `MessageQueue` 各送一則唯一訊息。在該程序內記錄各佇列的單調時鐘送出起點與首次 listener 送達經過時間（Java `System.nanoTime`、Rust `Instant`）。每個佇列從自己的送出起點量測，允許 callback 先於 send 返回；證據包含 client/run/topic/broker/queue/message 鍵值。分開的 topic 避免已消費 offset/積壓污染下一種語言的執行。
- 新增 `tests/differential/check_p10_fanout.py <evidence-dir> <topic-base>`，要求兩種語言各具完整且唯一的 16 佇列 assignment/send/delivery 集合，驗證事件順序與 5 s 送件/送達界限，並輸出各佇列經過時間及統計摘要。不得跨程序相減時鐘，也不要求精確 Java/Rust 延遲比率或接近零的中位數倍數。barrier 是回歸閘門；延遲是有界的輔助證據。

**Runner 接線**

以 `rg -n 'P10-V03|P11-V04|P9-V03|implemented=|code_dirty_paths|topic_queue_count|group_created' tests/differential/run.sh` 盤點；更新所有適用的派發位置，不可無差別修改每次出現處：

- manifest 輸入防護、`implemented`、profile/topic/group 白名單（`host-routed:P10-V05`），以及支援 ID 拒絕鏈；
- 實際 host-routed topic/subgroup 建立、16 佇列覆寫、命令紀錄、執行兩個探針與比對器的**可執行**場景分支、退出/結果/狀態記錄，以及失敗與成功時的程序/topic/group 清理；
- 新比對器的 source-dirty 計算（既有 `compat/java-reference/` 與 `integration-tests/` 目錄項目已涵蓋探針）。

在 Phase 10 matrix 新增 `P10-V05` ownership 列，並在 `CURRENT_STATUS.md` 解釋待執行案例。比對器新增合成證據自測：有效執行、期限前僅四個 pull、送件 barrier 前就收到回應、缺少/重複佇列，以及無效時間，都必須得到預期成功/失敗結果。live runner 必須與 manifest 條目在同一次實作變更完成。

## 5. 驗證方式

實作檢查（以下是未來實作要求，不代表本次計畫審查已修改程式碼）：

```sh
cargo fmt --all -- --check
cargo test -p rocketmq-classic-consumer push::pull_service::tests --locked
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -f compat/java-reference/pom.xml verify
bash -n tests/differential/run.sh
bash tests/differential/run.sh --list
bash tests/differential/run.sh --check-all
python3 tests/differential/check_p10_fanout.py --self-test
git diff --check
```

使用倉庫錨定的 Java/Maven 環境；記錄環境限制，不得宣稱未執行的檢查通過。證明 hold-response 整合測試在舊四 worker dispatcher 下失敗、在替換後通過。保留啟動回滾、慢 listener 關閉、明確等待值、pause/resume、快取流控、rebalance 世代隔離與 transport 清理測試。workspace 檢查包含編譯新 Rust 探針；Maven 驗證編譯 Java 探針。

最後由負責人執行的 Phase 19 閘門：

```sh
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed bash tests/differential/run.sh --scenario P10-V05
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all
```

透過既有證據機制，在倉庫外封存兩種語言日誌與比對結果。live 執行記錄結果前，`P10-V05` 維持待執行，不得把先前 37/37 live 結果視為涵蓋新案例。live 執行仍屬 Phase 19 義務，不阻擋本地驗證完成的實作合併。

## 6. 驗收標準

1. 不再有額外固定 Pull 派發寬度/通道容量。穩定指派 `N` 個符合條件的佇列且 transport 許可充足時，peer 在任何回應之前看見 `N` 個目標 topic 在途 pull，每佇列一個。移除溢位重試；保留 transport 送件控制與真正的快取流控。
2. 每個已送件請求擁有一個 guard，並被追蹤到 join。完成、owner 失效、panic、取消、首次 poll 前 abort 都會釋放一次槽位。deferred 後續請求與新舊世代保持隔離；已完成任務保留量不隨迭代次數增長。
3. stop 關閉送件、取消、於既有局部期限內 join，再 abort/drain 落後者。回滾/關閉保留既有未解決訊息與 offset。測試以仍運作的 scheduler/保留的觀察依據證明清理，不能只靠已清空的狀態計數。
4. Push 選用既有 async Pull API 及其清理語意。Pull header 使用錨定的 15 s Broker 掛起與 30 s client timeout；POP 保留 20 s 輪詢。公開 API/設定及其他 Java 對應流控/重試延遲不變。
5. ADR、雙語計畫/SVG、相容性列與 rlog 都描述有條件的扇出及生命週期契約，不宣稱 Java transport 無限併發或整個 phase 已達相容。
6. `P10-V05` 具完整可執行的 host-routed runner、隔離探針、含負面自測的比對器與清理。離線 manifest/runner 檢查通過；執行前 live 結果保持 `PENDING_PHASE_19`。

## 7. 決策

- 保留既有 dispatcher 與啟動階段名稱，避免無關更動。
- 移除 Pull 專屬固定上限；保留既有 transport 限制。穩態扇出隨符合條件的指派數擴展；rebalance 期間退役世代可短暫共存。
- 使用 `JoinSet` 所有權與 spawn 前 guard，取代自行移除的 `HashMap<PullIdentity, JoinHandle<()>>`，避免任務脫離管理或漏掉早期 abort 清理。
- 保留已記錄的負責人決策（2026-11-12），新增永久 `P10-V05`。修正理由：本地腳本 peer 可確定性地偵測此缺陷；live 案例以真實 Broker 確認相同行為，且必須在發布訊息前先以空佇列併發建立閘門。
