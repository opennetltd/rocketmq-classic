# 計畫：Phase 4 相容性覆蓋收尾

> 主計畫：[`../09/00-rocketmq-classic-plan.md`](../09/00-rocketmq-classic-plan.md)，Phase 4 實作計畫：[`../09/05-phase-04-typed-headers-bodies.md`](../09/05-phase-04-typed-headers-bodies.md)。
> 本限定範圍後續工作用於補齊 Phase 4 typed-header/body 證據缺口。第 7 節與 Phase 4 matrix 記錄已完成結果；此處只驗收 typed wire-model gate，不代表後續 RPC 或 live Broker 覆蓋已完成。

## 1. 目標與範圍

完成已實作 Phase 4 header 與 body 對 Java 5.5.1 的可執行相容性覆蓋、關閉已確認的模型／oracle 缺口，並將觀察到的結果更新至 Phase 4 matrix 與 append-only session log。以下所有驗收條件達成前，Phase 4 維持未完成。本工作不擴及 RPC 執行或 live broker 驗證。

## 2. 收尾稽核開始時的狀態（2026-10-01）

- `crates/rocketmq-classic-protocol/tests/headers.rs` 涵蓋 Java-to-Rust fixture 及部分 required/invalid header case。Rust-to-Java Maven 驗證會比較 19 個 Rust 產生 header frame 的所有 ext field，但尚未證明每個已實作 header 都有完整雙向覆蓋。
- 七個 Rust 產生的 body fixture 會透過 `mvn verify` 與 Java 5.5.1 oracle 做語意比較。
- `docs/compatibility/REQUEST_CODES.md` 記錄剩餘 partial：Reply V2/push、POP Lite response、consumer-list response fixture、lock/unlock body oracle，以及 ACK Lite body/source audit。
- `rlogs/2026/09/05-phase-04-typed-headers-bodies__20261004T021500Z.rlog` 記錄先前命令與結果。保留其 append-only 歷史。
- 目前工作樹包含使用者／session 改動，必須保留。每次編輯批次前檢查 `git status --short`，不得還原無關變更。

## 3. 實作步驟

### 3.1 補齊 header fixture 覆蓋（P4-V01）

1. 盤點 `crates/rocketmq-classic-protocol/src/header/basic.rs` 中所有已實作 `CommandHeader` 及其在 `header/mod.rs` 的公開匯出。對照 `compat/java-reference/src/main/java/compat/Main.java`、`crates/rocketmq-classic-protocol/tests/headers.rs` 與 Maven Rust-frame verifier 的 Java fixture stem 及 assertion。
2. 每個已實作 request 與 response header 均須具備 pinned Java 5.5.1 fixture、逐欄位檢查所有 model field 的 Rust decode assertion，以及由 Java 解碼並精確比對預期 ext field 的 Rust 產生 frame。對 Java 編碼時略過 null field 或使用 primitive default 的情況，也要增加 omission fixture/assertion，不可只測填滿欄位的正例。
3. 覆蓋清單必須由測試 inventory 驅動（可用 checked-in table 或等效完整 case list），使新增已公開 header 卻缺少任一方向測試時測試失敗。除非找到 pinned Java custom-header call site，否則不得為 ACK_LITE 宣稱有 header model。

### 3.2 完成 table-driven validation 覆蓋（P4-V02）

1. 在 `crates/rocketmq-classic-protocol/tests/headers.rs` 建立逐 header case，列出 valid baseline fields、required field 集合、數值欄位、boolean 欄位及 Java fixture 關聯。涵蓋所有含 required field 的 header，包括 response header。
2. 對每個 required field 分別移除並檢查精確的 `ProtocolError::MissingField` 欄位名稱。每個數值欄位測試格式錯誤與超出 Rust 型別範圍的值；適用時測試 signed 邊界。optional 欄位須區分 missing、empty、zero、false。boolean parsing 需對照 Java `Boolean.parseBoolean` 行為。
3. 保留 raw ext-field preservation 測試，並在複製的 map 上執行 typed encode；確認 unknown entry 原樣保留。若單一 table row 可表示，就不要重複撰寫 header 專屬 ad hoc test。

### 3.3 對齊剩餘模型／oracle 缺口

1. **Reply V2 與 push：**檢查 pinned `rocketmq-all-5.5.1` header class、conversion helper 與 `MQClientAPIImpl` call site。只為 source 證實且實際使用的 request/response shape 增加 typed model 及 Java fixture；比較完整欄位與適用的 message-body 語意。若 shape 不是 custom header，明確記錄並另行測試 body。
2. **POP Lite response：**檢查確切 Java response header/body class 與 call site。實作缺少的 typed response model、fixture 產生、decode assertion 及 Rust-to-Java assertion；保留 binary message body 邊界。
3. **Consumer-list response：**使用實際 Java `ConsumerListByGroupResponseBody` 編碼產生 `consumerIdList` 為 null/absent、空集合及有值的 Java 5.5.1 fixture。測試 Rust 語意 decode/encode，且不可混淆 absent 與 empty。
4. **Lock/unlock body oracle：**產生 `LockBatchRequestBody`、`UnlockBatchRequestBody`、`LockBatchResponseBody`、`UnlockBatchResponseBody` 的 Java 5.5.1 request/response JSON fixture。比較 Rust 輸出語意並測試 collection/omission 行為；繼承的 header field 仍由 header fixture 覆蓋。
5. **ACK Lite audit：**檢查 ACK Lite request body 建構及 response handling 的 source 與 call graph。僅在實際 Java body 存在時加入有 source 依據的 body model/test。若 client 傳入 null custom header，就不建立 header model。於 matrix 記錄 source file、method/call-site 證據與明確結論。

### 3.4 同步相容性證據

依完成的 case 更新 `docs/compatibility/REQUEST_CODES.md`，列出 fixture ID、測試方向、model 狀態及剩餘缺口。Model ready 與 RPC implemented 分開記錄。對應自動化 gate 實際執行前，Phase 4 verification checkbox 維持未勾選。於既有 rlog 追加日期項目（不可改寫舊行），記錄變更路徑、fixture provenance、case ID、完整命令／結果及任何殘留 TODO/PARTIAL。

## 4. 驗證

開發時先執行 focused check，完成後執行所有 repository gate：

```bash
cargo test -p rocketmq-classic-protocol --test headers --locked
cargo test --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -q -f compat/java-reference/pom.xml verify
python3 tests/differential/compare.py --self-test
git diff --check
```

Maven `verify` 必須能在 generated fixture 目錄為空時按設定重新產生 Rust fixture inputs。回報實際命令結果；不得依既有 fixture 或先前 session log 推定通過。

## 5. 驗收條件

- P4-V01 透過可執行的完整 inventory：每個已實作 request/response header 都有 Java-to-Rust 全欄位 assertion 與 Rust-to-Java 精確欄位 assertion。
- P4-V02 以 table-driven 測試覆蓋適用 header 的 required、數值格式錯誤、overflow、boolean、optional value 與 unknown-field preservation case。
- P4-V03 持續通過 normalized Send V1/V2 header 與 semantic JSON body fixture 測試，包括 omission 和 collection 差異。
- 每個已列模型／oracle 缺口均以 pinned-source fixture/test 關閉，或附 source 證據明確列為 unresolved 並標示 blocking；不得臆造不受支援的 ACK Lite header。
- Compatibility matrix 與 append-only rlog 必須符合已執行證據。若任一必要 case pending、partial 或缺少可執行 oracle，Phase 4 維持未完成。

## 6. 限制與待釐清證據

使用 pinned Java client 5.5.1 作為 wire-format oracle。不得依推論或 server 限制更改 protocol 行為。若 source annotation 與 call-site 行為不一致，測試／matrix 保留差異，並以實際 Java 建構／解碼路徑釐清。不需要 Docker、live broker、新 test framework 或無關的 Phase 5 RPC 工作。

## 7. 完成紀錄（2026-10-04）

P4-V01/V02/V03 的收尾工作已完成。完整 inventory 涵蓋 `basic.rs` 每個公開 `CommandHeader` 及 unlock alias，並具備雙向檢查；Maven verifier 解碼 38 個 Rust 產生的 header frame。Table-driven Rust test 涵蓋每個已建模 header field、required/optional case、格式錯誤與邊界數字、Java boolean parsing，以及 unknown-field preservation。Body gate 語意檢查 14 個 Java 解碼 frame，包含 consumer list absent/empty/populated、lock/unlock body，以及 reply V2/push binary body bytes。Pinned ACK Lite 稽核未找到獨立 Lite body/header call path；非 null request header 會走共用 ACK_MESSAGE method。

最終證據：`cargo test -p rocketmq-classic-protocol --test headers --locked`、`cargo test --workspace --locked`、`cargo fmt --all -- --check`、strict workspace/all-target/all-feature Clippy、`mvn -q -f compat/java-reference/pom.xml clean verify`、`python3 tests/differential/compare.py --self-test` 及 `git diff --check` 均通過。Append-only 執行紀錄保存 fixture provenance 與結果。剩餘 RPC wiring、public client surface 及 Broker integration 屬後續 phase 範圍，不是 Phase 4 typed-model acceptance 缺口。
