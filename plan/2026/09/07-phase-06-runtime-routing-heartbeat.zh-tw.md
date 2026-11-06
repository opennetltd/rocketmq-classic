# Plan: Phase 6 — Client runtime、routing 與 heartbeat

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 6；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`07-phase-06-runtime-routing-heartbeat.md`](07-phase-06-runtime-routing-heartbeat.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

依 client identity 管理與共享 runtime resource，提供一致 routing 與 heartbeat snapshot，並證明完整 startup rollback/shutdown。

## 2. 前置條件與現況分析

Phase 5 RPC 已驗收。consumer pull/rebalance 尚未實作；runtime 只提供必要 registration 與 wakeup contract，不在此建立 Consumer scheduler。

- 先完成: [Phase 5](06-phase-05-typed-rpc-api.zh-tw.md).

來源對照修正（2026-10-09）：Pinned `MQClientManager.getOrCreateMQClientInstance` 先查 client-ID table，已存在時直接回傳；只有 key 不存在才會進入 `computeIfAbsent`。後續 config/hook 不會取代或觸發 first-runtime conflict check。原本的 `localServeAddr` 文字屬於 Proxy `ProxyConfig.initData`，在 client-only phase 範圍之外；此處應使用來源確認的 `ClientConfig.clientIP`/`NetworkUtil.getLocalAddress` 行為。詳見[需求稽核](../../docs/compatibility/PHASE_6_REQUIREMENT_AUDIT.md)。

### 2.1 必讀 Java 參考

- 上述來源對照修正已依 pinned 5.5.1 artifact 驗證；改動以下行為前，仍須依執行索引的 pinned-source 規則確認各 class/method 與實際 call site。
- `MQClientManager`
- `MQClientInstance`
- `ClientConfig`
- `TopicPublishInfo`
- `ClientRemotingProcessor`
- `RebalanceService`
- `PullMessageService`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-core/src/config.rs
crates/rocketmq-classic-core/src/runtime/manager.rs
crates/rocketmq-classic-core/src/runtime/runtime.rs
crates/rocketmq-classic-core/src/runtime/tasks.rs
crates/rocketmq-classic-core/src/route/cache.rs
crates/rocketmq-classic-core/src/heartbeat.rs
crates/rocketmq-classic-core/tests/runtime.rs
docs/architecture/ADR-0002-client-runtime-sharing.md
```

ClientRuntimeManager 是明確可共享 handle，不是不可重設的 global singleton。後續 facade 共享 default manager，測試可指定 manager。client-ID entry 管理 single initialization，並遵守 Java first-mapping 行為：相同 ID 再次呼叫時回傳既有 runtime，即使後續 config/hook 不同；保留首次 runtime 的 config/property/hook snapshot，且不修改後續 caller config。不得因後續同 ID 呼叫的 config 衝突而拒絕。建立 Created→Starting→Running→ShuttingDown→Stopped 與 startup failure。group registration 依 identity 管理，不依 Arc strong count。

## 4. 依序實作步驟

### 4.1 P6-01 / P6-02 — 建立 identity 與 single-flight creation

以 fixture 測試 ClientConfig ID，包含 instance/unit 組成，並測試來源確認的 automatic `clientIP` 初始化。`ProxyConfig.localServeAddr` 屬於 server，不在此 client phase 範圍。先以 per-key OnceCell 或 entry 取得 ownership，再建立 socket/task。不同 key 可並行初始化。初始化失敗只移除自己的 entry，依文件允許 retry。在 ADR-0002 明確記錄 runtime ownership。

### 4.2 P6-03 / P6-04 — 實作 lifecycle 與 group lease

start/shutdown transition 序列化，文件指定處需冪等。producer/consumer registration 回傳 identity token 用於 rollback/unregister；依 Java 拒絕重複 group。一個 producer shutdown 不可停止其他 registration 仍使用的 runtime。避免 registry 與 runtime handle 形成 strong-reference cycle。

### 4.3 P6-05 / P6-08 — 發布一致 route snapshot

fetch 時不持有 cache lock，先完整轉換 route，再替換 topic/broker/publish/subscribe 一致 snapshot。重用 Phase 1 conversion，包含 static/ordered route。master/slave publish/subscribe resolution 與 VIP port transformation 使用純函式，含 port 檢查與 IPv6。除非 Java 要求，暫時 refresh failure 不應丟棄有效 cached route。

### 4.4 P6-06 / P6-07 — 管理 periodic task 與 heartbeat snapshot

集中預設值：route poll 30000 ms、heartbeat 30000 ms、offset persistence 5000 ms；由 Java 確認 initial delay 與 missed-tick 行為。各 task 皆有 cancellation 與 join owner。offset tick 只呼叫已註冊且存在的 consumer persistence capability。於鎖外以 snapshot 建構 heartbeat；測 subscription version、V1/V2 設定、unregister 及 producer-only/consumer-only 組合。

### 4.5 P6-09 / P6-10 — 註冊 inbound slot 並處理 failure rollback

依來源流程路由 inbound：check-transaction 依 producer/body group；consumer-change notification 喚醒整個 runtime 的 rebalance（header group 僅供記錄）；reset-offset、consumer-status、running-info、direct-consume 依 consumer group；reply-push 依 correlation identity。負責 phase 啟用 feature handler 前回傳明確 protocol error，不能假裝成功。在 registration、transport start 及各 task start 後注入 failure，反向撤回已取得資源並 join。最後一個 registration shutdown 應在有界 deadline 內，先向 Broker unregister 再關 transport。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P6-V01** — 同 ID 大量並行 get_or_create 只產生一個 identity/task set/socket pool；不同 ID 不被全域序列化。
- [x] **P6-V02** — stale failed creation 與 unregister token 不得移除 replacement；重複 ID 必須回傳 Java 首次建立的 runtime，且後續設定不同時仍保留首次 config/hook。
- [x] **P6-V03** — paused-time timer、route snapshot reader 及全部 startup-failure point 不留下 partial state 或 task leak。

- [x] **P6-U01** — scripted response 與 paused time 驗證 heartbeat/subscription/unregister bytes，以及共享使用者獨立 shutdown。

### 5.1 實作完成後的本機命令

已於 2026-10-10 在 default-topic route API 直接 differential、有序 NameServer provider 對齊及 nullable NameServer address accessor oracle 後重驗。locked workspace/all-target tests、strict all-target/all-feature Clippy、pinned offline Maven verify（22 個場景與 cross-decodes）、comparator self-test/manifest22、format 與 diff checks 均通過。這些是本機檢查，不含 live Broker 執行；**P6-V04 仍是 Phase 19 handoff**，不是 Phase 6 gate。較廣的 compatibility audit 仍將 Linux/Windows/BSD 實際 runtime address enumeration 及剩餘 source/input branches 列為 open。各 ID 證據與 fixture provenance 見[需求稽核](../../docs/compatibility/PHASE_6_REQUIREMENT_AUDIT.md)。

```bash
cargo test --workspace --all-targets --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -o -q -Dmaven.repo.local=/private/tmp/p6-maven-repository -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
python3 tests/differential/compare.py --validate-manifest
cargo fmt --all -- --check
git diff --check
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P6-V04** — 本機 Broker 可見正確 heartbeat/subscription change 與 unregister；一個共享使用者 shutdown 後另一個仍可使用。

## 6. 驗收與交接

Phase 7 接收 lifecycle registration、route resolution 與 shared transport ownership。stub handler 不可標為 COMPATIBLE，matrix 必須註明啟用 phase。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 7](08-phase-07-producer-core.zh-tw.md).
