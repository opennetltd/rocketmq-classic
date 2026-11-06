# Plan: Phase 3 — TCP/TLS Remoting transport

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 3；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`04-phase-03-remoting-transport.md`](04-phase-03-remoting-transport.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

提供受 deadline 限制的 invoke/oneway 與 inbound dispatch；每個 request 只有一個 terminal owner，所有 task 有明確 shutdown。

## 2. 前置條件與現況分析

Phase 2 cross-codec 檢查已通過。使用 scripted peer 與暫存 TLS certificate；race 與 failure 則以 loopback scripted peer 進行可重現測試。

- 先完成: [Phase 2](03-phase-02-remoting-codec.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `NettyRemotingClient`
- `NettyRemotingAbstract`
- `ResponseFuture`
- `InvokeCallback`
- `TLS configuration and channel events`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-remoting/src/client/mod.rs
crates/rocketmq-classic-remoting/src/client/connection.rs
crates/rocketmq-classic-remoting/src/client/connection_manager.rs
crates/rocketmq-classic-remoting/src/client/pending.rs
crates/rocketmq-classic-remoting/src/client/processor.rs
crates/rocketmq-classic-remoting/src/client/tls.rs
crates/rocketmq-classic-remoting/tests/transport.rs
docs/architecture/ADR-0001-remoting-transport.md
```

invoke(addr, command, deadline) 回傳一個 future；invoke_oneway 不插入 pending response。pending entry 包含 connection generation、deadline 與 oneshot sender。map 可由短暫同步鎖保護，但 guard 不得跨 await；remove 就是 terminal ownership 操作。每個 connection 一個 reader 與無界 outbound enqueue/writer path，加上受管理 timeout scheduler 即足夠；不可每個 request 建立 timeout task。採 Tokio time，讓測試可確定性推進 deadline。

## 4. 依序實作步驟

### 4.1 P3-01 / P3-02 — 管理與重用 connection generation

序列化 write；outbound frame enqueue 不設硬容量上限並保留 join handle，遵循 Java Netty `writeAndFlush` 行為，不額外加入 Rust 專屬 queue-full 拒絕。address key 正規化不得破壞 IPv6、DNS/SNI 或 TLS identity。同 key 採 single-flight creation 並分享健康 connection；stale close 只可移除自己的 generation。creation 被取消時需關閉未公開 socket，不同 address 則可並行連線。

### 4.2 P3-03 / P3-04 — 讓 pending ownership 具原子性

opaque 使用 wrapping atomic generation；wrap 時不得覆寫仍 pending 的相同 ID。先 insert 再 write，以免漏掉立即 response。response、deadline scan、caller cancellation、write failure、close 與 shutdown 皆透過同一函式 remove-and-complete。connection failure 只移除該 generation。late/duplicate response 忽略並限制診斷量；invoke 取消後應立即移除 pending entry。

### 4.3 P3-05 / P3-06 — 所有階段共用一個 deadline

admission、connection acquisition、TCP/TLS handshake、socket write 與 response wait 共用同一 deadline。outbound enqueue 無界，不等待 queue capacity。已逾時 invoke 不得送出。區分 connect、timeout、closed 與 malformed-frame error。oneway 設定對應 bit，以無硬容量上限方式 enqueue，並遵循 Java 的非同步 write failure 處理；不宣稱 Broker 已收到或儲存成功。

### 4.4 P3-07 — 將 inbound request 移出 reader 處理

先區分 response 與 request，再查 pending。依 raw code 將 request 交給受管理 processor workers，queue admission 須符合 Java executor backlog 行為（Java 採無界 queue 時，Rust 亦不設人為容量上限）；response 保留原 opaque 並設定 response bit。不支援的 request 回 Java 相容 unsupported code，但 inbound oneway 不得回覆。依 Oracle 定義 rejection 行為；不得靜默丟棄已接受的 transaction/reply 工作。

### 4.5 P3-08 — 實作具驗證的 TLS

使用 tokio-rustls/rustls，正常驗證 trust chain 與 hostname。plain TCP 與 TLS 由設定明確選擇，不做不安全自動 fallback。提供本機 CA trust input；選定 profile 需要時支援 client certificate。保留 DNS name 供 SNI；connect/handshake 沿用原 deadline。production default 不得內嵌 test private key。

### 4.6 P3-09 — 關閉並 join 全部受管理工作

拒絕新工作、取消 creation、關閉 connection、清空 pending、停止 processor/timer 並 join task。重複 shutdown 必須安全，task 不得等待自身 join。在 ADR-0001 記錄 Java flow mapping 與 ownership；任何可觀察差異都阻擋 parity 並需審查，不以 intentional-difference 豁免。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P3-V01** — 以 barrier 控制 response/timeout/cancellation/shutdown race，最多完成一次且 pending 為零；包含 opaque wrap。
- [x] **P3-V02** — old-generation close 不得移除 replacement；並行 creation 只產生一個 connection；無界 outbound enqueue 可超過先前 queue 容量，socket write 阻塞仍遵守 operation deadline。
- [x] **P3-V03** — loopback inbound request、unsupported/oneway 行為及並行 response 正常，reader 不被阻塞。

- [x] **P3-U01** — scripted TCP/TLS peer 驗證 raw route framing、CA 信任／不信任與 hostname 失敗、deadline 及 shutdown 後零 owned task。

以上四項勾選只記錄本機自動化測試證據，不代表 Phase 3 已驗收。Java/Rust executor 與 callback lifecycle review 仍於 inventory 標示 Tokio 排程／所有權差異；P3-V04 仍延後至 Phase 19。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-remoting --test transport --locked
cargo test -p rocketmq-classic-remoting --locked
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P3-V04** — 本機 Broker raw route invoke 成功；本機 TLS trusted CA 成功，wrong hostname/untrusted CA 失敗；shutdown 後 task/socket 為零。

## 6. 驗收與交接

transport 不包含 Producer retry 或 RocketMQ correlation table。Phase 4 使用 protocol 所有的 traits；Phase 5 接收 deadline-based single-attempt transport。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 4](05-phase-04-typed-headers-bodies.zh-tw.md).
