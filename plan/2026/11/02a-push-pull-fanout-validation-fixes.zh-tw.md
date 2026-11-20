# 計畫 02a — 補齊 Push Pull 扇出驗證缺口

## 1. 目標與範圍

延續 Plan 02，不變更其派發架構，也不改寫歷史證據。審查基準：`889901e9`（2026-11-13 審查）。修正永久 `P10-V05` 探針/比對器與測試資源所有權，使實作符合 Plan 02 §§4.5–4.7 及驗收標準 6。

自有 `JoinSet`、spawn 前 guard、async Pull 選擇，以及分開的 Pull/POP 掛起常數均已存在。本地腳本 peer 扇出測試通過。live 執行仍由負責人執行並維持 `PENDING_PHASE_19`；尚未執行本身不算偏離計畫。目前發現不要求變更正式派發程式。

## 2. 已確認問題

| 優先級 | 證據 | 偏離 / 影響 |
| --- | --- | --- |
| P1 | `compat/java-reference/src/main/java/compat/LivePullFanoutProbe.java:83,110` | `routeQueues` 在 `producer.start` 前呼叫 `producer.fetchPublishMessageQueues`。本地快取的 pinned 5.5.1 原始碼顯示 `DefaultMQProducerImpl.fetchPublishMessageQueues` 呼叫 `makeSureStateOK`，拒絕非 RUNNING 狀態。live Java 探針無法到達 gate；編譯與參數 smoke test 無法發現此錯誤。 |
| P2 | `tests/differential/check_p10_fanout.py::check_language` | 未比對 GATE 時間與送出時間；收集 SEND_END 卻不要求存在、唯一或匹配佇列/時間。審查產生的「移除全部 SEND_END」及「GATE 前發布」fixture 都錯誤通過。無法落實發布前空 topic 契約或完整成功送出證據。 |
| P2 | 兩個探針 recorder（`FanoutProbeState`、Java `Recorder`） | gate 計算歷史開始數，未追蹤 active 佇列請求或拒絕重複 active start。回應違規只在後續 start 才推論，未於回應時記錄，且 gate 分次讀取狀態。兩個 listener 在輸出證據前都會隱藏重複訊息 key 送達，使宣稱能偵測重複的比對器看不到它。啟動期限從 route 初始化後計算，未區分啟動與首次 Pull 起算的送件視窗。 |
| P2 | `crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs::broker_peer` 與最後 teardown；`tests/differential/run.sh` 群組建立 | Broker reader/writer/handler 任務脫離管理；teardown 只 abort accept 任務，且默許 join timeout，未證明 Plan 02 要求的全部 peer 任務完成。runner 在整個建立迴圈成功後才標記兩個群組已建立，若第二個建立失敗，便漏掉第一個的明確清理。 |

審查檢查：現有比對器自測通過 11 個合成案例；`run.sh --check-all` 通過，共 38 筆；`cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked` 通過（1 個測試）。這些成功無法關閉上述問題。比對器反例建立於暫存目錄；審查未修改正式程式或既有 fixture。

## 3. 實作計畫

### 3.1 修正 Java 探針資源取得與清理

- 依原意先啟動 consumer，再於 route 查詢前啟動 producer。以明確清理所有權涵蓋兩者取得，確保 producer-start、route、gate、send、delivery 失敗仍關閉所有已成功啟動的 client。移除後面重複的 producer start。
- Rust 同樣補上清理所有權：目前 `producer.start().await?` 位於 consumer 啟動之後、operation/cleanup 區塊之前；發生錯誤時仍須關閉 consumer。不得改公開 API 或 transport 政策。
- 使用 pinned API 或小型內部生命週期測試介面，加入針對 Java 取得順序的回歸測試，再由後續 live smoke run 證明。參數驗證與編譯本身不足。

### 3.2 讓探針狀態與證據忠實反映行為

- 兩個 recorder 都依目標佇列與請求識別碼維護 active map。佇列已有 active 請求時拒絕再次 start；回應須匹配 active 請求才能移除。維持精確 topic/group 過濾。
- 在同一狀態鎖內評估並鎖定 gate：預期 16 個佇列全都 active、尚無目標回應、距第一個目標 start 最多 5 s。gate 鎖定前若有回應，立即記錄違規。鎖定後允許正常回應與後續請求；不可僅因曾有回應，就把之後每個 start 都判為違規。
- 給啟動獨立的有界等待，並從第一個目標 start 起落實送件視窗。各程序採一致的單調時鐘起點，gate snapshot 必須原子化；不可跨程序比較時鐘。
- 每次目標送達都先輸出，再為進度追蹤去重 key。去重只用於判斷所有不同訊息是否已到達，讓比對器仍看得到重複證據。維持在 callback 進入時記錄首次時間。
- 新增 recorder 測試：gate 前回應但沒有後續 start、重複 active start、gate 鎖定後合法後續請求，以及重複送達仍輸出。ready file 仍只依有效且已鎖定的 gate 寫入。

### 3.3 比對器落實完整證據

- 解析 GATE 時間、數量與 span，對照已記錄 Pull start 驗證。gate 必須不早於最後一個初始 start，且早於任何目標回應；每個 SEND_START 必須不早於 gate。依時間戳驗證順序，不依輸出行的交錯順序。
- 每個發布 key/佇列必須各有一筆 SEND_START 與成功 SEND_END，匹配 queue/key、非負 queue offset，且 send-end 不早於 send-start。拒絕缺少、重複、未知或不匹配的 SEND_END。依 Plan 02，delivery 可早於 SEND_END，但不可早於 SEND_START 或晚於 shutdown。
- 要求完整匹配的 assignment/send/delivery 集合，保留每佇列關閉時最後一個合法未完成長輪詢。探針揭露重複後，拒絕重複 delivery。保留各程序時間量測與既有 5 s 界限。
- 擴充 `--self-test`：兩個已重現反例、gate 早於最後初始 start、最後初始 start 與 gate 之間已有回應、重複/不匹配 SEND_END、重複 delivery，以及合法 callback-before-send-return。每個負面 fixture 須因預期原因失敗，合法 fixture 須通過。

### 3.4 管理 peer 資源與部分初始化

- 腳本 peer 以自有集合保留全部連線 reader/writer/handler 任務。teardown 時關閉接收新連線、發出連線取消、join 全部子任務；以有界清理策略 abort/drain 落後者。不得默許 timeout。測試保留清理觀察依據，證明沒有連線任務存活；保留兩輪 hold-response 測試。
- `tests/differential/run.sh` 以陣列即時記錄每個成功建立的 fan-out 群組，成功/失敗時只清理該集合。嘗試下一個前先登記前一個。保留可丟棄環境防護與 topic 清理。
- 以 stubbed command runner 驗證群組部分失敗：第一次建立成功、第二次失敗、第一個會被刪除。驗證正常關閉與注入測試失敗時的 peer 清理。這些局部檢查不需 Docker。

### 3.5 證據與最後負責人執行

新增 `rlogs/2026/11/02a-push-pull-fanout-validation-fixes__<UTC>.rlog`；僅在修正驗證有依據時更新 ADR-0085 / 相容性證據。保留 Plan 02 與舊日誌。不得把先前記錄的 37 案例 live aggregate 改稱涵蓋 P10-V05。

本地檢查後，由負責人在可丟棄 host-routed Broker 執行 P10-V05，封存兩個探針日誌與比對結果。真實執行記錄結果前，live 狀態維持待執行。完整 aggregate 仍是最後 Phase 19 義務，不是撰寫本修正計畫的前提。

## 4. 驗證

```sh
python3 tests/differential/check_p10_fanout.py --self-test
bash -n tests/differential/run.sh
bash tests/differential/run.sh --check-all
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test -p rocketmq-classic-consumer push::pull_service::tests --locked
cargo test -p rocketmq-classic-integration --bin live_client --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -f compat/java-reference/pom.xml verify
git diff --check
```

透過各自測試 target 執行新增的生命週期、recorder 與 stubbed-cleanup 回歸測試。分開記錄 Maven 精確環境限制；上次執行日誌記錄 exact-JDK fixture 不符與補充 portable gate 通過，不代表標準 verify 通過。

最後由負責人執行：

```sh
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed bash tests/differential/run.sh --scenario P10-V05
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all
```

## 5. 驗收標準

1. Java 探針在 producer 為 RUNNING 時才查 route；兩個探針在每個部分取得失敗後都清理 client。
2. gate 以原子狀態與首次 Pull 起算視窗，證明發布前空佇列同時 Pull。重複 active 請求與重複送達皆可觀察且會失敗。
3. 比對器拒絕 gate 前發布與缺少/無效送出完成；合法 callback-before-send-return 與最後未完成長輪詢仍通過。
4. 每個 peer 任務都 join 或 abort/drain，並具可觀察完成依據；部分群組建立失敗會清理已建立群組。
5. 本地檢查與雙語文件一致；負責人提供執行證據前 live 結果維持待執行。正式派發行為與 Plan 02 所有權決策不變。
