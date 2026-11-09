# 修改計畫：全專案清理與可維護性審查

## 1. 目標、基準與限制

審查日期：2026-11-08。原始碼基準：`16386296`。英文版為準；本 `.zh-tw.md` 範圍相同。本次僅新增計畫。

已檢視七個函式庫 crate 與 `integration-tests` 的 workspace manifests、模組清單，針對 Consumer、Core、Producer、Protocol、Common、Remoting 追查原始碼及呼叫端；另檢視 examples、Java oracle 設定、fixtures 結構、scripts、differential runner、相容性現況、發布檢查表及既有計畫。這是涵蓋全專案的結構審查與重點分析，不代表逐行驗證每個檔案的語意，也不能證明所有公開項目是否被外部使用。

維持 Rust 1.85.0、edition 2024、Java client 5.5.1 相容性及 Broker 5.3.4 驗證基準。專案多處已採用現代寫法（`OnceLock`、`let else`、`is_some_and`、Tokio）；不因年代而重寫。本次未向線上 registry 查核依賴的新舊或安全性，不提出版本升級。

## 2. 發現與優先順序

以下路徑相對於 repository；行號以本次基準為準，實作時會變動。

| ID／優先度 | 依據 | 判定與預期結果 |
| --- | --- | --- |
| F01／P1 | `crates/rocketmq-classic-consumer/src/process_queue.rs:1`、`pull/result.rs:223,233,256`、`push/consumer.rs:2863`、`push/pull_service.rs:446,464` | 功能整合後仍保留早期階段註解及整檔／個別項目的 `allow(dead_code)`。Pull、Push 都有呼叫 filter-server routing；scheduler `pause` 也有正式呼叫端。移除過時抑制，改寫為目前的責任說明。 |
| F02／P1 | Consumer `process_queue.rs:1803`（`set_lock_state`）、`push/consumer.rs:2864`（`activate_process_queue`）、`push/pull_service.rs:465`（`resume`） | 編譯器指出三者未用於正式函式庫；查到的呼叫端是單元測試。以 `cfg(test)` 限制編譯範圍，保留測試。正式流程使用 `set_lock_state_at`、`activate_process_queue_with_lock` 及其他 scheduler 控制。 |
| F03／P2 | `crates/rocketmq-classic-core/src/runtime/registry.rs:207,773` | `GroupRegistry::clear` 僅由單元測試呼叫。`PendingReplyEntry.cancelled` 也僅在該方法讀取，其他 cancellation 欄位仍在使用。將此方法及此 entry 欄位限於測試編譯，保留取消測試及正式取消狀態持有者。 |
| F04／P2 | Core `src/java_number.rs:3,30`；Consumer `src/process_queue.rs:95,126`；Protocol `src/header/mod.rs`；Common `src/message/host_address.rs`、`src/permission.rs` | Java UTF-16 十進位數字表及帶正負號數字解析重複。先比較行為，再將等價的基本操作集中於 Common。保留符號、Unicode 數字、邊界與各呼叫端的錯誤轉換；Rust `str::parse` 並非等價替代。 |
| F05／P2 | `crates/rocketmq-classic-common/src/serde_fastjson.rs`、`crates/rocketmq-classic-protocol/src/serde_fastjson.rs` | 遞迴 JSON 字串與型別轉換重複，可能各自演進。Protocol 額外支援 `NumberToken` 與前導小數點，不能互換整套實作。僅抽取已證明相同的操作，保留專用解析。 |
| F06／P2 | Consumer `src/push/consumer.rs` 共 18,073 行，測試自 7,527 行起；Remoting `src/client/mod.rs` 共 5,856 行，測試自 3,036 行起；Protocol `src/header/basic.rs` 共 9,053 行，測試自 8,250 行起 | 正式行為與大量內嵌測試混在一起，增加審查成本。先將測試移入子模組，再依責任拆分正式程式碼，不擴大可見性或公開 API。 |
| F07／P3 | Producer `src/admin.rs:355`；Consumer `src/admin.rs:35` | epoch 毫秒飽和轉換重複，兩個 admin 模組的 `first_query_message` 也相同。屬小型重複；只有窄範圍 Core／Common helper 能降低整體複雜度時才整合。不強制所有 clock 函式共用政策。 |
| F08／P2 | `README.md` 重複歷史／當前驗證敘述；`docs/release-checklist.md` 共 11,345 行，含多次日期追記；已有 `docs/compatibility/CURRENT_STATUS.md` | 當前指引被歷史紀錄淹沒。以 current status 為權威來源，由精簡 checklist／README 連結；保留歷史稽核及各 revision 的驗證限制。 |

尚未確認有整個正式子系統或公開 API 可以移除。測試專用程式碼不需要進入正式編譯，但仍需保留以供驗證。F04–F07 是維護性改善機會，不是已證實的行為缺陷或已量測的效能改善。

## 3. 相容性界線

- 保留 deprecated transaction listeners／settings、consumer implementation accessors、header aliases：這些是刻意保留、有 fixtures／測試的 Java 相容性介面。相容性測試需要時保留 `allow(deprecated)`。
- 不因 variant 名稱重疊就合併 Producer 與 Consumer service-state enums：其順序、derive 與公開方法不同。
- 保留用於 trait objects、異質 futures、遞迴 async 建構的 boxed futures，不機械式改成 async trait methods。
- MD5／SHA1／DES 與 TLS 相容行為維持原狀，除非另有協定／安全性決策；本計畫不修改。
- 保留平台分支及有理由的 lint 例外、Java oracle fixtures、各 crate 的 LICENSE／NOTICE、歷史 `rlogs`。metadata checker 要求各 crate 保留授權副本。
- 不刪除階段命名的 wrapper，例如 `scripts/phase7-producer-scripted-tests.sh`：Phase 7 matrix 仍引用它。本計畫不要求 GitHub Actions workflow。

## 4. 實作階段

### 4.1 Phase A — lint 範圍與測試專用程式碼（F01–F03）

1. 修改前保留強制 dead-code 診斷。移除 Consumer 整檔抑制及過時的 routing／scheduler 個別抑制。
2. 在 F02 三個 helper 加上 `#[cfg(test)]`，保留內容與呼叫端。註解改為實際責任，不再描述未來階段。
3. 私有 `GroupRegistry::clear`、`PendingReplyEntry.cancelled` 及其初始化欄位加上 `#[cfg(test)]`；正式使用的 `PendingReply` 與 guard 取消旗標維持原狀。編譯後檢查條件式 imports。
4. 逐項判斷剩餘診斷，不以另一個模組級抑制掩蓋。平台／相容性例外有理由時保留並註明。
5. 執行 Consumer／Core 單元與整合測試、嚴格 Clippy。此階段獨立 commit，不混入檔案搬移。

### 4.2 Phase B — 共用數字語意（F04）

1. 比較上述五處實作的呼叫端與邊界，保留既有 Java 行為測試。
2. 將 UTF-16 digit lookup 與帶正負號十進位解析放入 `common/src/utils/java_number.rs`，透過 `#[doc(hidden)]` 模組提供最少量跨 crate helper。搬移既有 Core 實作，不新增 Common 對 Core／Protocol 的依賴。
3. 切換前比較新舊解析：空字串／只有符號、空白、ASCII 與全形／阿拉伯數字、混合數字、surrogate units、帶符號最小／最大值與溢位。對全部 65,536 個 `u16` 值完整比較 digit lookup。
4. 遷移 Core、Consumer、Protocol 及等價 Common 呼叫端；回傳寬度或錯誤語意不同時保留 wrapper。等價檢查通過後才刪除重複表；若新增 helper exports 納入相容性清單，更新對應紀錄。

### 4.3 Phase C — 模組整理（F06）

1. 使用 `#[cfg(test)] mod tests;`，將內嵌測試移至 `push/consumer/tests.rs`、`client/tests.rs`、`header/basic/tests.rs`。保留測試名稱、私有存取與 assertions，比較搬移前後發現的測試清單。
2. 分開 commit：將 Push admin／runtime adapters 及 assignment／consume 邏輯移到子模組；將 Remoting invocation／dispatch implementation blocks 移到子模組；basic headers 依 request／response families 拆分，由 `basic` re-export 原名稱。
3. 保留公開 import paths 與 macro 展開行為。僅在子模組需要時使用 `pub(super)`，不為測試而公開 helper。搬移時檢查 lock 生命週期與 awaits，避免意外改變。
4. 每次拆分執行受影響 crate 測試，再執行完整 workspace 檢查。搬移時不新增替代抽象、不重寫演算法。

### 4.4 Phase D — 有界限的去重（F05、F07）

1. Fastjson 比對 raw values、重複 keys、nulls、巢狀 arrays／objects、數字 keys、指數／suffix 形式與前導小數點 tokens，使用既有 fixtures。僅將相同的 JSON writing／scalar helper 抽到 Common；Protocol 可依賴 Common，反向不可。若抽象需要在整個 parser 傳遞 mode flags 或改變轉換結果，保留重複並記錄原因。
2. admin 程式碼可考慮以 hidden Core helper 提取第一個結果、Common helper 明確定義 epoch 毫秒飽和轉換。保留第一筆為 null 的行為及 Consumer 的三天回溯區間。維持 Instant deadlines 及不同時間／錯誤政策；若只為微量程式碼增加公開介面，略過抽取。
3. 執行 Common／Protocol fixture 測試、三個 admin test targets 及 Maven oracle 驗證。不重新產生 fixtures 來讓行為改變通過測試。

### 4.5 Phase E — 文件與最終驗證（F08）

1. 在 `CURRENT_STATUS.md` 保留目前 revision、具日期的驗證限制及 MAN-01–MAN-04；README 重複敘述改為簡短連結，保留使用說明。
2. 分開目前發布 checklist 與歷史章節。歷史內容原文移入具日期的 archive 文件；維持 anchors／links，或提供明確的搬移連結對照。不將過去 PASS 主張默默改成目前結果。
3. 執行以下完整本機驗證。涉及 runtime 重構時，由 owner 依既有 Docker／differential 流程執行受影響的 Broker 5.3.4 scenarios，記錄 source revision 與輸出位置。實際執行前不得宣稱發布核准或完整 live 重驗完成。

## 5. 驗證方式與本次檢查

本次審查已執行以下命令，皆成功；警告為預期診斷發現：

```bash
cargo rustc -p rocketmq-classic-consumer --lib --locked --offline -- --force-warn dead_code
cargo rustc -p rocketmq-classic-core --lib --locked --offline -- --force-warn dead_code
cargo rustc -p rocketmq-classic-remoting --lib --locked --offline -- --force-warn dead_code
```

Consumer 恰好回報 F02 三個方法；Core 回報 F03 欄位及方法。Remoting 僅回報 `ParsedProxyRoute::MissingAddress` 的 `username`／`password`；依 `client/config.rs:155–165` 註解，這是刻意保留的 Java 畸形 proxy 設定狀態，本計畫保留。正式函式庫診斷本身不能證明公開 API 無人使用。本次未執行完整測試、Clippy、Maven 或 live scenarios。

實作階段的檢查：

```bash
cargo fmt --all --check
cargo test -p rocketmq-classic-consumer -p rocketmq-classic-core --locked --offline
cargo test --workspace --all-features --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
python3 scripts/check-workspace-metadata.py
mvn -o -f compat/java-reference/pom.xml verify
git diff --check
```

離線檢查使用快取依賴；若缺少快取，記錄該 gate 受阻，不宣稱 PASS。F06 以前後 `cargo test -p <affected-package> --locked -- --list` 比較測試清單。F04–F06 完成後，在已設定的主機執行既有文件中的 Linux／Windows cross-target 檢查；僅 macOS 編譯不能驗證那些分支。Owner 執行的 live checks 是最後實作 gate，涵蓋 Pull／Push rebalance、orderly handoff、request/reply cancellation、環境支援時的 class-filter routing，以及所修改路徑的 remoting deadlines／shutdown。採用既有 scenario 定義，不臆造 ID。

## 6. 驗收條件

- 僅將已辨識的測試 helper 排除於正式編譯；保留全部測試覆蓋。
- 移除 Consumer 過廣 dead-code 抑制及過時未來階段註解；剩餘例外有當前理由。
- 共用基本操作保留 Java Unicode／overflow／null／coercion 行為；無法證明等價時保留專用實作。
- 公開 API paths、wire output、lifecycle、cancellation、offset 行為保持相容；Rust 1.85.0 與 crate 依賴方向維持有效。
- 測試搬移前後發現的測試一致。不因文字搜尋找不到引用就刪除依賴或 fixtures。
- 中英文保持同步；歷史證據可存取；最終紀錄區分已執行檢查及等待 owner 的檢查。
