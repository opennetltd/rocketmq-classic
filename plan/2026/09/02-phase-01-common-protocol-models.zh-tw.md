# Plan: Phase 1 — 共用型別、常數與協定模型

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 1；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`02-phase-01-common-protocol-models.md`](02-phase-01-common-protocol-models.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

建立最小可建置的 Rust workspace，以及不遺失資訊的 message/protocol 模型。本階段不包含 network I/O、Producer retry 或 Consumer scheduling。

## 2. 前置條件與現況分析

Phase 0 必須已驗證基準並產生 model fixtures。workspace 尚不存在；現在只建立 common 與 protocol crates。其餘 crates 在負責的 phase 才新增，不建立空殼。

- 先完成: [Phase 0](01-phase-00-reference-oracle.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `Message`
- `MessageExt`
- `MessageQueue`
- `MessageConst`
- `MessageDecoder (properties only)`
- `MessageSysFlag`
- `PullSysFlag`
- `LanguageCode`
- `SerializeType`
- `RequestCode`
- `ResponseCode`
- `TopicRouteData`
- `HeartbeatData`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
Cargo.toml
Cargo.lock
rust-toolchain.toml
crates/rocketmq-classic-common/src/lib.rs
crates/rocketmq-classic-common/src/message/
crates/rocketmq-classic-common/src/sysflag/
crates/rocketmq-classic-protocol/src/lib.rs
crates/rocketmq-classic-protocol/src/codes/
crates/rocketmq-classic-protocol/src/route/
crates/rocketmq-classic-protocol/src/heartbeat/
```

使用 edition 2024，將經驗證支援的 toolchain 固定於 rust-toolchain.toml；記錄選定 MSRV，不可只由 edition 猜測。初期設定 publish=false。protocol 依賴 common，禁止反向依賴。bytes、serde/serde_json 與 thiserror 僅在實際使用時加入，版本於實作時解析並鎖定。request/response code 與 opaque 為 i32；offset/timestamp 與 broker ID 使用 Java 相容的 signed width。wire model 與具驗證的 user constructor 分開，保留 sentinel value 表達能力。

## 4. 依序實作步驟

### 4.1 P1-00 — 建立並驗證 workspace

建立 root manifest 與兩個實際 library crate，package 名稱遵循主計畫。統一 workspace dependency 版本並提交 Cargo.lock。加入 Rust 內建測試、fmt/clippy/test CI 及 dependency direction 的 package metadata 檢查。沒有具體用途前，不加入 dashmap、parking_lot、compression backend 或 async framework。

### 4.2 P1-01 — 確立 primitive 與 unknown-value 規則

在 wire model 附近記錄位寬。使用 raw integer code 或含已知常數的 newtype，不使用會拒絕未知 request 的封閉 enum。可保留未知值的 tagged type 提供 Unknown(raw)。在轉為 usize 前測試負值 sentinel 與 min/max；長度配置前必須驗證。

### 4.3 P1-02 / P1-03 — 移植 language 與 serialization 映射

對照 pinned source 稽核每個 LanguageCode，包含 JAVA=0 與 RUST=12。保留未知 language byte。SerializeType 包含 JSON=0 與 ROCKETMQ=1；未知 tag 可用於診斷，但不可聲稱能解碼。marked-length 算術由 Phase 2 負責。

### 4.4 P1-04 — 分離 user message 與 decoded message

實作 Message、MessageExt 與 MessageQueue，以 private invariant 與 accessor 管理狀態。MessageExt 保存 broker offset、host、timestamp、sysFlag、reconsume count 與 ID，不可窄化。MessageQueue equality/hash/order 遵循 Java 欄位語意；Java 字串排序為 UTF-16 code-unit 順序，需測試 supplementary Unicode，不可假設等同 Rust String::cmp。這些值不得包含 transport state。

### 4.5 P1-05 — 編碼與解析 message properties

實作前先以 fixtures 記錄 Java separator 常數及 empty/malformed/duplicate entry 行為。共用一套 property encoder/parser。已知 system property 名稱集中為常數，保留 unknown property。user validation 不得靜默截斷 separator 或覆寫 reserved field。只有受控 property 順序才比對 bytes，否則比對 decoded map。

### 4.6 P1-06 — 集中管理 flags

移植 client 相關 MessageSysFlag/PullSysFlag mask 與 helper。此處 CompressionType 只描述 flag value；實際壓縮／解壓縮在 Phase 7/8 加入。測試 combined flag、清除 bit 與未知 reserved bit，確保不改動其他 bit。

### 4.7 P1-07 / P1-08 — 實作 route 與 heartbeat 模型

依 Java fixtures 明確設定 serde field name 與 omission 規則。保留 broker-ID map key、ordered/static-topic metadata、permission，以及 subscription expression/version。撰寫純函式 publish/subscribe conversion，涵蓋缺 master、不可讀／寫 queue 及 static-route。只有使用端需要 Java 排序時才排序，不虛構 JSON set 順序。

### 4.8 P1-09 — 完成 numeric registry

移植完整 pinned RequestCode/ResponseCode 清單，不只主計畫範例表。與 Phase 0 清單比對名稱／數值；有意的 alias 應註記，不可拒絕全部重複數值。更新 REQUEST_CODES.md 的未實作狀態；有常數不代表已支援 RPC。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P1-V01** — raw code、signed 邊界、LanguageCode 與 SerializeType 映射可 round-trip 且不遺失欄位。
- [x] **P1-V02** — property golden case 涵蓋 separator、Unicode、empty input、duplicate 與 unknown key。
- [x] **P1-V03** — Java queue 排序／allocation input fixture 與 route/heartbeat JSON 語意一致，包含 static metadata。
- [x] **P1-V04** — Phase 0 的每個 model 欄位都能表達，package dependency 檢查無循環。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo metadata --format-version 1 --no-deps
```

## 6. 驗收與交接

Phase 2 接收已驗證 value type 與常數，不包含網路邏輯。在 matrix 記錄精確 source provenance 及 unknown JSON-field 保留規則。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 2](03-phase-02-remoting-codec.zh-tw.md).
