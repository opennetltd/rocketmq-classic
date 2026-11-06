# Plan: Phase 5 — MQClientApi typed RPC facade

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 5；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`06-phase-05-typed-rpc-api.md`](06-phase-05-typed-rpc-api.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

在 Remoting 上建立 single-attempt domain RPC，使本機 integration/differential test 可獨立於 Producer 或 Consumer runtime 執行。

## 2. 前置條件與現況分析

Phase 3–4 的 typed header 與 transport 已驗收。message binary decoder 到 Phase 8 才建立：PullResultExt 先回 raw pull bytes，不在此重寫 decoder。

- 先完成: [Phase 4](05-phase-04-typed-headers-bodies.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `MQClientAPIImpl`
- `MQBrokerException`
- `MQClientException`
- `PullResultExt`
- `SendResult`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-core/Cargo.toml
crates/rocketmq-classic-core/src/api/mod.rs
crates/rocketmq-classic-core/src/api/route.rs
crates/rocketmq-classic-core/src/api/producer.rs
crates/rocketmq-classic-core/src/api/consumer.rs
crates/rocketmq-classic-core/src/api/transaction.rs
crates/rocketmq-classic-core/src/error.rs
crates/rocketmq-classic-core/tests/rpc.rs
tests/differential/run.sh
```

MQClientApi 擁有或借用 RemotingClient，不依賴 process singleton。Broker method 接受明確 address、typed input 與 absolute deadline，不含 producer retry loop。NameServer selection/failover 依 Java route 路徑，且使用同一原始 deadline。各 method 先分類 response code，再解碼 success-only field。structured error 保留 raw response code、remark、request code 與 peer；不得記錄 credential 或 message body。

## 4. 依序實作步驟

### 4.1 P5-01 — 建立 core 與明確 error conversion

新增向下依賴 remoting/protocol/common 的 core crate。區分 malformed protocol、connect、closed、timeout、Broker/NameServer response、invalid config 與 unsupported feature。保留 error source 與 raw unknown code。以 scripted loopback peer 測試各 mapping，不為 mock 新增只有單一實作的 transport interface。

### 4.2 P5-02 — 實作 route lookup

以 typed header 建立 GET_ROUTEINFO_BY_TOPIC，解碼 TopicRouteData。涵蓋 existing/missing topic、首個 NameServer 失效、empty server list 與 malformed response。不得靜默製造 writable route。Producer route creation fallback 是 Phase 7 policy，不可偽裝成 route RPC 成功。

### 4.3 P5-03 — 實作 producer 單次呼叫

新增 send V1/V2、batch 與 end-transaction method。依 Java 映射 SEND_OK 及 storage/slave status，保留全部 SendResult identifier 與 region/trace field。實作 sync/future/oneway transport 選擇，不挑 queue 或重試 message。end transaction 的 invocation mode 由 Java call site 確認。

### 4.4 P5-04 / P5-07 — 實作 consumer method 與逐 RPC status map

新增 pull、query/update offset、consumer list、send-back 與 lock/unlock。pull 回傳 status、next/min/max、suggested broker 與 raw binary body；non-found status 不可當作 generic error。offset-not-found 與 transport failure 分開。遵守 Java oneway offset/unlock mode 及逐操作 success response shape。

### 4.5 P5-05 / P5-06 — 新增 lifecycle 與 extension RPC

實作 heartbeat/unregister，以及 source 確認有使用時的 heartbeat V2 negotiation。以 Phase 4 model 加入 Lite/assignment/request-mode/POP/ACK/change-invisibility lower-level call；尚未 decode 的 message 回 raw bytes。即使 public exposure 未決，各 family 仍需盤點；不能把 Broker 不支援 response 當成相容證據。

### 4.6 P5-08 — 讓 RPC 證據可執行

以 scripted peer 與 Java fixture 建立 table-driven RPC request/status/error test。在 tests/differential/scenarios.yaml 登錄 live case ID、required profile、比較規則與 execution_phase=19。Phase 19 建立 integration package 與 live differential runner；一般 cargo test 不依賴 Docker。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P5-V01** — 每個 method 皆測 scripted success、預期 domain outcome、unknown error code 及 malformed success response。
- [x] **P5-V03** — 單次 Broker method 只送一個 attempt；NameServer failover 與 oneway 遵守文件中的 deadline/pending 規則。

- [x] **P5-U01** — 以 Java request/response fixture 覆蓋每個 RPC 成功／錯誤分支及精確 invocation mode，不依賴 live server。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-core --test rpc --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P5-V02** — 每個 supported method 都在 pinned 本機 Broker/NameServer 執行，具備 Java success/error differential 證據。

## 6. 驗收與交接

core API 可在沒有 public Producer/Consumer class 時執行。Phase 6 接收 route/heartbeat/unregister primitive；Phase 8 將 message decode 接到既有 raw pull result。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 6](07-phase-06-runtime-routing-heartbeat.zh-tw.md).
