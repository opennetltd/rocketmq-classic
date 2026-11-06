# Plan: Phase 2 — Remoting frame codec

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 2；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`03-phase-02-remoting-codec.md`](03-phase-02-remoting-codec.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

與 Java Oracle 交叉解碼完整 Remoting frame，涵蓋分段及惡意輸入。codec 必須獨立於 live socket。

## 2. 前置條件與現況分析

Phase 0–1 已驗收。現在即擴充 transaction 與 request/reply command 的 Oracle fixtures，public API 留待後續 phase。

- 先完成: [Phase 1](02-phase-01-common-protocol-models.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `RemotingCommand`
- `RocketMQSerializable`
- `RemotingSerializable`
- `NettyEncoder`
- `NettyDecoder`
- `FastCodesHeader`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
crates/rocketmq-classic-remoting/Cargo.toml
crates/rocketmq-classic-remoting/src/command.rs
crates/rocketmq-classic-remoting/src/error.rs
crates/rocketmq-classic-remoting/src/codec/frame.rs
crates/rocketmq-classic-remoting/src/codec/json_header.rs
crates/rocketmq-classic-remoting/src/codec/rocketmq_header.rs
crates/rocketmq-classic-protocol/src/header/mod.rs
crates/rocketmq-classic-remoting/tests/codec.rs
```

完整 frame 的 total_length = 4 + header_length + body_length，不包含第一個 length word。framing integer 皆為 big-endian。設定一個最大完整 frame size，明確記錄是否含 prefix（內部採包含 prefix），在 reserve/split 前驗證。HeaderMap、ProtocolError 與 header conversion trait 放在 protocol，而非 remoting：Phase 4 的 protocol header 不得向上依賴 remoting。RemotingCommand 在 typed decode 後仍保留 raw ext_fields map。

## 4. 依序實作步驟

### 4.1 P2-01 — 新增 command 與 codec error

新增 remoting crate，僅依賴 protocol/common 及實際使用的 codec dependencies。RemotingCommand 擁有 code/language/version/opaque/flag/remark/ext_fields/body/serialization；response 與 oneway helper 只修改自己的 bit。預設 language 為 RUST。Java 區分 absent 與 empty remark/body 時必須保留；若 wire 等價而合併，需記錄。

### 4.2 P2-02 — 讀取與配置前先驗證長度

total length 與 24-bit header length 使用 checked arithmetic。拒絕 total_length 小於 4、signed-negative／不可能長度、header 長於 payload、不支援 serialization，以及超過 configured limit。資料未收齊回傳 NeedMore，而非 malformed frame。encoder 也必須拒絕無法放入 Java wire width 的欄位。

### 4.3 P2-03 — 實作 JSON header

使用精確 Java field name 與 value type，包含 LanguageCode 的 JSON 表示，不可假設 binary byte 在 JSON 也是數值。保留 unknown extFields string。Java→Rust 與 Rust→Java 雙向解碼；semantic JSON 比對不要求 object-key 順序。

### 4.4 P2-04 — 實作 private header layout

實作前由 RocketMQSerializable 抄錄並記錄各欄位 width/order，包含 code/version narrowing、remark length 及 ext-field key/value length width。只在有界 header slice 內讀取；遇截斷、無效字串編碼或 nested length 不一致時回錯，不可 panic。large request code 必須與 Java private-codec 行為比對，不得靜默窄化；無法使用此 codec 的操作需採 Java 支援的 serialization 路徑。

### 4.5 P2-05 — 定義共用 header 邊界

在 protocol 定義 encode_fields(&mut HeaderMap) 與 decode_fields(&HeaderMap)，回傳 typed validation error。先以一個有 fixture 的小型 header 驗證，完整 header 集留給 Phase 4。typed decode 借用 raw map，不消耗或替換 command map。不需要 reflection 或另一份 unknown-field bag。

### 4.6 P2-06 / P2-07 — 先完成 incremental decode，再考慮最佳化

只有對後續 transport 有用時才實作 tokio-util Decoder/Encoder；BytesMut parsing 本身維持可測試。測試每個 byte 切點、多 frame 及尾端未完整 frame；未完整 input 不得被消耗。可簡單達成時採 bounded direct buffer write。以 Java optimized FastCodesHeader 路徑與一般 encoder 產生等價 fixture；即使 Rust 沒有獨立 fast path，兩者也必須交叉解碼為相同語意。延後 alternate fast-header path 時加 `ponytail:` 註解：先確保 golden-correct codec，Phase 18 量測有證據才新增 specialization。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P2-V01** — 依 Java codec 允許範圍，以兩種 serialization 交叉解碼 route/heartbeat/send/pull/offset/lock/transaction/reply fixtures。
- [x] **P2-V02** — 測試 length 0/1/0xFFFFFF/overflow、frame limit ±1、high-bit total length、nested truncation 與 unknown field/code。
- [x] **P2-V03** — 每個 fragmentation point 與 concatenated-frame case 只產生預期 commands；畸形 bytes 不得 panic 或造成無界 reserve。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo test -p rocketmq-classic-remoting --test codec --locked
cargo test -p rocketmq-classic-protocol --locked
mvn -f compat/java-reference/pom.xml verify
```

## 6. 驗收與交接

Phase 3 接收有界 codec 與 raw command type。將 Rust 產生的 fixture files 接入 Maven verify 供 Java 解碼；只有 Rust round trip 不算通過。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 3](04-phase-03-remoting-transport.zh-tw.md).
