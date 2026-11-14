# 計畫 02b — 對齊扇出事件順序與失敗清理

## 1. 目標與範圍

審查基準：`142881db`。補齊 P10-V05 recorder、比對器與腳本 peer 測試尚未完成的 Plan 02a §§3.2–3.4 要求。保留正式 Pull dispatcher、錨定版本、公開設定、Plan 02/02a 與歷史日誌。這些局部修正不需新增圖表。

Java producer 已於 route 查詢前啟動，成功建立的群組會立即登記，兩個 listener 都會輸出重複送達，先前 gate 前送出/缺少 SEND_END 的比對器缺陷也已修正。依計畫允許，live P10-V05 仍為 `PENDING_PHASE_19`；本次審查未執行 live。

## 2. 剩餘問題

### 2.1 P2 — Recorder 時間戳可能與原子 gate 矛盾

`integration-tests/src/bin/live_client.rs:839–861` 在取得 recorder 鎖之前取樣 `now`，再用該值更新 start 清單及鎖定 GATE。並行 Rust Pull 任務取得鎖的順序可能不同於取樣順序。例如，一條任務取樣 100 後等待；另一條取樣 200 並先記錄 start；延遲的任務接著湊齊 active 集合，卻把 GATE 鎖定於 100。比對器會正確拒絕此 gate，因為涵蓋的 start 位於 200。這是由原始碼確認可能發生的交錯，不代表已重現 live 失敗。

`earliest_start` 同樣記錄首個取得鎖者，不一定是最早取樣的 start。回應時間與 `try_latch_gate(now)` 也採鎖外時間取樣。Java `LivePullFanoutProbe.Recorder` 同樣在 synchronized 區段外取樣。Plan 02a 要求原子 gate 狀態與一致的單調時鐘證據；只鎖 map 無法確立時間戳順序。

### 2.2 P2 — 比對器接受超出送件視窗的 gate

`tests/differential/check_p10_fanout.py:179–193` 驗證首次 start 的 span 與 GATE 下界，卻未檢查首次 start 到 GATE 的經過時間。審查產生的 fixture 中，全部 start 接近 1 s、GATE 位於 11 s、回應延後，且 gate 後具有效 send/delivery，仍在 5 s 送件視窗下錯誤通過。探針本身有嘗試限制；比對器卻未獨立落實 Plan 02a 承諾的證據契約。

### 2.3 P2 — Peer 失敗清理仍會丟棄未完成 handle

`crates/rocketmq-classic-consumer/tests/push_pull_fanout.rs:93–98` 把 `JoinHandle` 移入 `timeout`，到期後丟棄未完成 handle 並 panic，使任務脫離管理。`join_recorded_tasks` 隨即在第一次 panic 時遺棄其餘 vector。主測試只有在初始化與扇出斷言全部成功後才進入明確清理；abandoned-connection 測試要求的是正常合作式清理，沒有測到 teardown 前斷言失敗。因此尚未證明 Plan 02a 要求的注入測試失敗後清理全部子任務。task witness 也在 spawned body 內建立，首次 poll 前 abort 時無法釋放已增加的計數。

## 3. 實作計畫

### 3.1 序列化 recorder 狀態轉換及其時間戳

- 定義 Pull 事件時間為 recorder 狀態轉換時間。先取得狀態鎖，再取樣單調時鐘，於同一臨界區更新 active/start/response 狀態與鎖定 gate。返回時間戳及不可變 rows/snapshot，解鎖後輸出。持有狀態鎖時不得呼叫 output sink。
- 對 Rust `pull_start`、`pull_end`、正式 gate-check 路徑，以及 Java `Recorder.pullStart`、`pullEnd`、`tryLatchGate` 套用此政策。確保 gate 不早於涵蓋的任何 start，且記錄的首次 start 為送件視窗起點。保留 callback 進入時的 delivery 時間與各程序獨立時鐘。
- 確定性時鐘注入只供私有測試使用。新增受控並行測試：一個呼叫在取鎖前延遲，另一個先記錄；驗證證據能通過比對器，且不會產生早於涵蓋 start 的 GATE。涵蓋回應與最後初始 start 競爭：它不是使 gate 前 barrier 失效，就是發生於有效鎖定後，狀態與證據必須一致。
- 保留重複 active-start 偵測、gate 後合法後續請求、完整 delivery 輸出及獨立啟動期限。不得放寬比對器以接受矛盾時間。

### 3.2 直接檢查 gate 經過時間

- `check_language` 除既有 start-span、response-order、send-order 檢查外，要求 `gate_at - min(first_starts.values())` 非負且最多 `admission_window_ms * 1_000_000`。先比較奈秒，再為報告捨入數值。
- 擴充 `--self-test`，加入已重現的延遲 gate fixture、恰好等於期限的合法 gate，以及剛超出期限的 gate。這些案例的回應與送出都放在 gate 後，確保斷言測到 gate 經過時間，而非其他順序錯誤。
- 保留 callback-before-send-return 與最後合法未完成 Pull。繼續拒絕 Plan 02a 的反例；不得改變 5 s 契約或跨程序時鐘政策。

### 3.3 先 drain peer 所有權，再回報失敗

- 等待期間保有 handle 所有權，以可變 handle 引用執行 timeout。逾時時保留失敗原因、abort 該 handle 並 await 完成。即使一條任務逾時或 panic，也須 drain 全部其他已記錄任務；全部 handle 結果皆已觀察且 live witness 歸零後，才回報累積錯誤。
- spawn 前建立 `LiveWitness` 並移入 future，使首次 poll 前 abort 能釋放已增加的計數。加入直接 early-abort 回歸測試。
- 重整扇出測試，使 setup/scenario 失敗無法繞過清理。peer 所有權置於可失敗 scenario body 外，擷取其結果/panic，發出取消、join accept 任務、drain 全部子任務，最後才傳遞 scenario 失敗。保留 Drop abort 作為非預期提早展開的後備措施，一般失敗路徑則須非同步 drain。
- 新增持有開啟連線時注入斷言失敗的測試，以及多個子任務下強制清理逾時的測試。兩者都須在回報原失敗前，證明全部 handle 已 drain 且 witness 為零。保留既有兩輪 hold-response 與 abandoned-connection 回歸測試。

### 3.4 證據與最後負責人驗證

新增 `rlogs/2026/11/02b-push-pull-fanout-ordering-cleanup__<UTC>.rlog`，記錄比對器反例、並行測試、失敗清理測試與實際命令結果。僅依修正保證更新 ADR-0085 / 相容性證據。保留先前計畫與 rlog 原文。

最後由負責人執行 P10-V05，透過既有證據機制封存兩個探針日誌與比對器結果。真實 Broker 執行記錄結果前，維持 `PENDING_PHASE_19`；先前 37 案例 aggregate 不涵蓋本案例。

## 4. 驗證

本次審查實際執行：比對器自測（20 案例）、runner 自測、runner `--check-all`（38 筆）、腳本 peer 整合測試（2 個通過），以及 Rust live-client 測試（7 個通過）。全部通過；新增的延遲 gate 反例仍錯誤通過。本次審查未重跑完整 workspace 或 Maven 閘門。

實作要求：

```sh
python3 tests/differential/check_p10_fanout.py --self-test
bash tests/differential/run.sh --self-test
bash tests/differential/run.sh --check-all
cargo test -p rocketmq-classic-consumer --test push_pull_fanout --locked
cargo test -p rocketmq-classic-integration --bin live_client --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
mvn -f compat/java-reference/pom.xml verify
git diff --check
```

將新增 Java recorder 回歸測試納入 `LivePullFanoutProbeFixtures` 及既有 Maven execution。依 Plan 02a 分開記錄 exact-JDK fixture 限制與補充 portable 驗證。

最後由負責人執行：

```sh
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable ROCKETMQ_PROFILE=host-routed bash tests/differential/run.sh --scenario P10-V05
ROCKETMQ_CLASSIC_TEST_ENV=local-disposable bash integration-tests/run-local.sh --all
```

## 5. 驗收標準

1. 並行 recorder 呼叫不會產生早於涵蓋 start 的 GATE；狀態與輸出證據中的 response/gate 順序一致。
2. 比對器拒絕超過送件視窗才到達的 gate，包含已重現的 10 s 案例，且不放寬舊檢查。
3. 斷言失敗、子任務 panic、清理逾時、首次 poll 前 abort，都不殘留未觀察的 peer handle 或 live witness；清理後才回報失敗。
4. 既有本地回歸仍通過，英文/繁中計畫一致；負責人提供執行證據前 live 狀態維持待執行。正式派發與公開行為不變。
