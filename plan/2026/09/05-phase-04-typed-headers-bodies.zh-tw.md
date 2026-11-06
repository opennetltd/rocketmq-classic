# Plan: Phase 4 — Typed request header 與 protocol body

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 4；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`05-phase-04-typed-headers-bodies.md`](05-phase-04-typed-headers-bodies.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

以 typed codec 取代已知 header 的 string-map 建構，同時保留 raw command field 與精確 Java validation 行為。本階段不執行 network call。

## 2. 前置條件與現況分析

Phase 0–3 已驗收；Phase 2 已將 header trait 與 ProtocolError 放在 protocol。擴充既有 trait，不可建立 protocol→remoting 依賴。

- 先完成: [Phase 3](04-phase-03-remoting-transport.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `remoting protocol/header and protocol/body packages`
- `SendMessageRequestHeaderV2 conversion helpers`
- `CommandCustomHeader validation`
- `MQClientAPIImpl request construction and response decoding`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-protocol/src/header/
crates/rocketmq-classic-protocol/src/body/
crates/rocketmq-classic-protocol/src/rpc.rs
crates/rocketmq-classic-protocol/tests/headers.rs
docs/compatibility/REQUEST_CODES.md
fixtures/remoting/
```

建立逐操作表格：code、request header、response header、body codec、required/optional field、oneway 支援與 fixture ID。requiredness 必須同時參考 Java header validation 與實際 call site，不只 annotations。missing、empty、zero 與 false 是不同情況。所有 Java 名稱，包括 V2 短 key，都需 fixture 支持。conversion error 指明欄位與預期型別，但不得洩漏 secret。

## 4. 依序實作步驟

### 4.1 P4-01 — 完成共用 conversion contract

使用 Phase 2 encode/decode trait。只抽出重複使用的 field-parsing helper，遵守 Java boolean/numeric string 行為並檢查 overflow。從借用的 HeaderMap 解碼；unknown field 留在原 RemotingCommand。測試將 header 重新編入 raw map 副本，確認修改 known field 不會刪除 unknown field。

### 4.2 P4-02 — 完整實作第一波 RPC 模型

依序實作 route、send V1/V2、batch send、pull、query/update offset、heartbeat、unregister、consumer list 及 lock/unlock。request 與 response header 都要建立：Phase 5 需要 send ID/queue offset、pull next/min/max/suggested broker，以及 offset-query result。重用 Phase 1 route/heartbeat body。由 Java conversion 擷取 V1↔V2 mapping 及 optional send/batch field，不猜測字母對應。

### 4.3 P4-03 — 新增 consumer extension wire model

實作 pinned client 實際使用的 send-back、assignment/request-mode、Lite/POP/ACK/change-invisibility 與 notification/polling 結構。依 source call graph，不假設 LITE_PULL_MESSAGE 就是一般 DefaultLitePullConsumer 路徑。只有實際 client call site 存在才納入 batch ACK。public exposure 不支援狀態與 wire-model 完整度分開記錄。

### 4.4 P4-04 — 新增 transaction、reply 與 runtime inbound model

實作 end/check-transaction、reply-send/push header 與 body shape。加入後續 processor 啟用所需的 notification、reset-offset、consumer-status 與 direct-consume model，尚不實作 handler 行為。correlation/reply property name 放在 common constant module。區分 ext-field serialization、message binary body 與 JSON body。

### 4.5 P4-05 — 逐一以 Oracle 稽核 known header

每個已實作 header 都產生 Java 正反例 fixture。以 table-driven Rust test 驗證 canonical map、absent optional、missing required、bad number、boolean 邊界與 signed 邊界。更新 REQUEST_CODES.md，區分 model ready 與 RPC implemented。除非小型且可稽核表格明顯能消除重複定義，否則不引入 code generation。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P4-V01** — 每個 header 皆有 Java→Rust fixture 與 Rust→Java decode assertion，包含 response header。
- [x] **P4-V02** — unknown ext field 在 typed 修改後仍保留；所有 invalid/missing required field 在 protocol boundary 失敗。
- [x] **P4-V03** — V1/V2 等價 send header 正規化結果相同；JSON body 保留 collection 與 omission 語意。

驗收證據：[Phase 4 相容性矩陣](../../../docs/compatibility/REQUEST_CODES.md)記錄完整 header inventory、38 個由 Java 解碼的 Rust header frame、14 個語意 body frame，以及 ACK Lite 的來源判定。[Phase 4 收尾紀錄](../10/01-phase-04-compatibility-closure.zh-tw.md#7-完成紀錄)與 append-only [執行紀錄](../../../rlogs/2026/09/05-phase-04-typed-headers-bodies__20261004T021500Z.rlog)記錄 pinned fixture provenance 與通過的命令結果。此處驗收 Phase 4 typed wire-model gate，不代表後續 RPC 或 live Broker 覆蓋已完成。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-protocol --test headers --locked
cargo test -p rocketmq-classic-remoting --locked
mvn -f compat/java-reference/pom.xml verify
```

## 6. 驗收與交接

Phase 5 可不透過 ad-hoc string map 建立全部 primary request。選配 family 缺證據時維持 TODO/PARTIAL 並阻擋該 family，不阻擋無關 basic RPC。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 5](06-phase-05-typed-rpc-api.zh-tw.md).
