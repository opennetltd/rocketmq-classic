# Plan: Phase 0 — 凍結參考基準與測試 Oracle

> 主計畫：[`00-rocketmq-classic-plan.md`](00-rocketmq-classic-plan.md)，Phase 0；執行索引：[`README.zh-tw.md`](README.zh-tw.md)。
> 英文為主版本：[`01-phase-00-reference-oracle.md`](01-phase-00-reference-oracle.md)。此文件是實作計畫，不是已完成／已驗證的報告。

## 執行契約

- **完整移植 Java client 5.5.1 的設定、功能與控制流程**，包含預設值、驗證、callback、error、retry 與 lifecycle。Java source/method/configuration 與 Rust、unit-test ID 必須雙向對照；不可新增 Rust 專有公開設定／功能，也不可缺少 Java client 能力。Rust 命名／ownership 可不同，可觀察語意不可不同。測試接點保持 private。
- **只實作 client**。Java client 5.5.1 維持 Oracle；**RocketMQ server V5_3_4（5.3.4）**是 production／本機 Docker 基準，不是 client source 基準。server 限制不能作為刪除 client 功能的理由；兩種 client 對同一 server 比較，無法執行的成功路徑仍保留 source／fixture 證據。
- **Phase 0–18 以 unit test 為主要測試方式**，輔以 Java golden／離線 differential，以及 transport 必要的小型 scripted loopback test。live Broker 或 Docker 都不是階段前置條件。Broker、TLS/auth cluster、跨語言、端對端與 server stress 執行集中至 [Phase 19](20-phase-19-docker-local-validation.zh-tw.md)；非本地檢查於該階段列為 owner 手動驗證。階段驗收不等於最終相容性／發布驗收。

## 1. 目標與範圍

在 Rust 協定實作前建立可離線執行的 Java Oracle 與完整 source／設定／功能清單。Docker cluster 建立及 live scenario 執行屬於 Phase 19。

## 2. 前置條件與現況分析

儲存庫目前只有主計畫與 SVG 圖，尚無 Cargo workspace、Java harness、fixtures 或測試。主計畫的 release/tag/commit 是指定基準，不是已獨立驗證的證據。

- 先完成: [執行索引](README.zh-tw.md).

### 2.1 必讀 Java 參考

- 實作前，依索引的 pinned source 規則確認以下 class/method 與實際 call site；此計畫未獨立驗證 Java artifact。
- `RemotingCommand`
- `RocketMQSerializable`
- `MQClientAPIImpl`
- `MQClientInstance`
- `DefaultMQProducerImpl`
- `PullAPIWrapper`
- `ProcessQueue`
- `RebalanceImpl`
- `RemoteBrokerOffsetStore`
- `LocalFileOffsetStore`

## 3. 目標檔案與實作契約

以下皆為相對 repository root 的預定新增／修改路徑；目前並不存在 Rust/Java 實作。依前面 phase 已建立的檔案進行最小修改，不重新建立平行版本。

```text
compat/java-reference/pom.xml
compat/java-reference/README.md
compat/java-reference/src/main/java/compat/Main.java
fixtures/README.md
fixtures/remoting/
fixtures/messages/
tests/differential/scenarios.yaml
tests/differential/compare.py
docs/compatibility/JAVA_5_5_1_COMPATIBILITY.md
docs/compatibility/REQUEST_CODES.md
docs/compatibility/KNOWN_DIFFERENCES.md
```

Oracle CLI 提供 `fixtures --output <dir>`、`decode --input <file>`、`scenario --id <id> --output <file>` 與 `self-test`。使用 Maven exec 執行 `compat.Main`；每個情境輸出一份 JSON。`scenarios.yaml` 使用 JSON 相容的 YAML 文件，讓 Python 標準函式庫不必加入 YAML dependency 即可讀取。每筆記錄 ID、所需 profile、負責 phase、受控輸入及精確的正規化規則。Python comparator 比較物件時忽略 key 順序，陣列則維持順序，除非特定欄位明確屬於集合。不得正規化掉錯誤碼、queue 分配、重試次數、順序或 offset 關係。

## 4. 依序實作步驟

### 4.1 P0-00 — 產生證據前先驗證基準

將 `rocketmq-all-5.5.1` 解析為 peeled commit，與 `efa1be59d6fc747ad220f036c86030f05f0419f9` 比對。解析 Maven `org.apache.rocketmq:rocketmq-client:5.5.1` 及對應 source artifacts。在 harness README 記錄 artifact SHA-256、tag/commit、JDK/Maven 版本及 source paths。若無法取得，不得改用 `develop`、其他 artifact 版本或未驗證的 Docker image；停止協定實作並請 owner 審查。

### 4.2 P0-01 — 建立證據與範圍清單

依主計畫建立 matrix，補上 Java setting name/type/default/unit/range/validation、public method/overload、source 控制流程分支、Rust mapping 與 unit-test ID。雙向稽核 Java-to-Rust 完整性及 Rust-to-Java 無額外公開功能／設定。納入 inherited、deprecated、environment/system-property 設定途徑。全部 client 能力均必要，包含 Lite/POP、allocation strategy、admin、namespace、hook、security 與 trace；不可選配刪減。

### 4.3 P0-02 — 實作協定 fixture 輸入輸出

使用 Java public API；若 release 必須存取 package scope，採用有文件說明的最小同 package adapter。盡可能明確設定 opaque、timestamps 與 properties，產生 JSON 與 ROCKETMQ commands。輸出完整 raw frame、解碼後 semantic JSON、body length 與 body SHA-256。decoder 接受兩種 serialization，遇到畸形輸入必須失敗。將 self-test 接入 Maven verify，不可依賴預設關閉的 Java assertions。

### 4.4 P0-03 — 建立可重現情境與 comparator

登錄主計畫全部 Phase 0 scenario ID，另加 heartbeat、offset update 與 header 反例。本階段實作離線 fixture/allocation/header scenario 與 comparator self-test。live route/send/pull/offset/lock scenario 記錄 execution_phase=19、required profile 及隔離 topic/group 要求；待執行 live case 不可回報 PASS。

### 4.5 P0-04 — 定義 Phase 19 環境需求

scenario metadata 分別記錄 server_version=5.3.4 與 java_client_version=5.5.1。盤點 basic、two-Broker、TLS/auth 及特定功能環境需求，交給 P19-01/02。本階段不建立／啟動 Docker，不要求 live server 證據。以 unit test 驗證 manifest 及缺少前置條件的回報。

### 4.6 P0-05 — 提交可重現證據

使用附錄 B 的 fixture 命名。每個 .bin 配一份 .json，包含 source version、scenario、serialization、request code、normalized fields、payload SHA-256 及 creation command；raw capture 與 normalized result 分開保存。本階段必須具備 route、heartbeat、send、pull、offset query/update 及 lock/unlock families。後續 phase 實作功能前先擴充清單。將 dependency/version check 接入 Maven verify，並記錄 fixture 重建方式及 Java 授權／NOTICE 來源。

## 5. 驗證

先寫對應失敗測試／Oracle fixture，再依序實作；以下 case 必須成為可執行檢查，而非人工勾選。以下檢查不依賴 Docker 或 live Broker，使用 Java fixture、可控時鐘及 scripted peer；共通命令與證據規則見執行索引。

- [x] **P0-V01** — 產生 fixtures 兩次；metadata hash 符合 raw file，正規化 semantic result 一致。
- [x] **P0-V02** — Java 可解碼自身產生的兩種 codec；畸形 fixture 失敗；comparator self-test 能找出有意義差異。
- [x] **P0-V04** — 修改 Maven client 版本會使驗證失敗；缺少前置條件的 scenario 不得回報 PASS。

- [x] **P0-U01** — 不啟動 Broker 即可建構 route、heartbeat、send、pull、offset 與 lock/unlock fixture；scenario metadata 區分 client/server 版本及待執行 live 證據。

### 5.1 實作完成後的本機命令

下列命令由本 phase 或前面的 phase 建立對應 entry point 後執行；此次文件拆分未執行或宣稱測試通過。

```bash
mvn -f compat/java-reference/pom.xml verify
mvn -f compat/java-reference/pom.xml exec:java -Dexec.mainClass=compat.Main -Dexec.args="fixtures --output target/java-fixtures"
python3 tests/differential/compare.py --self-test
```

### 5.2 Phase 19 live 測試交接

保留以下 case ID 及行為要求，於 Phase 19 使用 5.3.4 Docker server 執行；不是本 phase 的通過條件，不可提前標 PASS。

- [ ] **P0-V03** — 基本本機 cluster：Java producer/consumer、route、heartbeat、pull、offset 與 lock 操作產生非空的 machine-readable 證據。

## 6. 驗收與交接

一併提交已驗證基準、fixture manifest、離線 Oracle 證據與完整 source/configuration matrix。離線門檻通過後開始 Phase 1；Docker、live scenario 及非本地手動驗證需求交至 Phase 19，不在本階段啟動 cluster。

- 交接需附變更檔案、case ID、實際命令／結果、fixture provenance，以及 matrix 狀態與未完成項目；共通規則見索引。
- 下一階段: [Phase 1](02-phase-01-common-protocol-models.zh-tw.md).
