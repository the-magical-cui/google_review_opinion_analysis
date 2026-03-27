## 2026-03-14
* [16:57] 初始化專案結構，建立 `data/`、`scripts/`、`src/google_place_review/`、`AGENTS.md`、`LOG.md`、`REPORT.md`、`README.md`、`requirements.txt`。
* [16:57] 建立 Google Maps reviews scraper prototype，規劃以 Selenium 為主、JSONL 為原始輸出格式。
* [17:53] 補強 Google Maps place URL 解析與 debug artifact，加入 URL、place name、selector hit、screenshot、HTML、debug JSON 的紀錄能力。
* [18:50] 將主要輸出整理到 `data/raw/rebirth/`，並保留 `data/raw/_legacy/` 作為早期測試資料夾。
* [19:20] 修正 review card 展開與 `review_text` 擷取邏輯，開始使用 `data-review-id` 作為重要識別線索。

## 2026-03-15
* [09:05] 新增 `--chromedriver-path` 與 `ScraperConfig.chromedriver_path`，允許明確指定 ChromeDriver。
* [13:45] 擴充 scroll strategy，比較 `baseline`、`scroll_into_view`、`action_chains`、`hybrid` 在 Rebirth 的效果。
* [13:47] 完成 Rebirth 初步策略比較，確認 `baseline` 類策略較容易抓到較多評論。

## 2026-03-24
* [23:25] 補強 reviews pane focus、scroll response、outer page scroll、map interference 等 debug 欄位，改善 baseline 行為診斷。
* [23:40] 將 baseline 由單純 page-down 類操作調整為更偏向直接控制 reviews pane 的互動路線。

## 2026-03-25
* [00:20] 加強 reviews tab 與 reviews pane 的 selector 驗證，確認 `button.hh2c6[role='tab']` 與 `div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde` 是重要結構線索。
* [00:45] 將入口主線調整為 `review-intent URL first`，並加入 tab fallback 與對應 debug 欄位。
* [02:45] 新增多策略 lazy-load runner，加入 `pane_step_scroll`、`last_card_into_view`、`action_chain_to_last_card` 與更完整的 round-level debug。
* [15:52] 回退 reviews 入口 gating，改為先允許進入 reviews context，再以 `entry_verdict` 與 `scrape_verdict` 區分是否進到完整評論列表與是否真的抓到評論。
* [16:20] 新增獨立的 Playwright probe：建立 `src/google_place_review_playwright/`、`scripts/run_playwright_probe.py` 與 `data/playwright/rebirth/runs/`，不替換 Selenium 主線。
* [20:25] 更新 `AGENTS.md` 文件規則，明確區分 `LOG.md`、`REPORT.md`、debug artifact 的用途。
* [20:34] 擴充 Playwright probe，新增 `search_then_click_place` 入口模式、pre-entry/post-tab 截圖、搜尋結果挑選與評論 tab 候選盤點。
* [21:40] 新增 Playwright 半自動接手模式，改用 persistent profile 啟動專用 Chrome，等待人工登入並開到評論頁後再接手。
* [22:15] 新增 Playwright 單輪抓取器 `scrape_once.py` 與 `run_playwright_scrape_once.py`，以半自動評論頁接手模式直接輸出單輪 JSONL。

## 2026-03-26
* [00:25] 新增 SQLite 匯入模組與 `import_jsonl_to_sqlite.py`，將 Playwright 單輪 JSONL 匯入 `places`、`scrape_runs`、`reviews` 三表。
* [00:27] 將 `owner_response_text`、`owner_response_date` 與 `has_owner_response` 納入 `reviews` 主表，其餘低頻欄位改收進 `low_frequency_json`。
* [01:05] 新增 Streamlit 單店檢視頁面與 SQLite 查詢模組，支援地點搜尋、時間圖表、評論分頁與基本篩選。
* [01:40] 調整 JSONL 匯入腳本，支援依 `place_id` 自動挑最新 Playwright run 並自動推回對應 debug 路徑。
* [01:42] 新增 `scripts/show_sqlite_summary.py`，快速檢查 SQLite 內各店的評論筆數、run 數與最新 run id。
* [09:30] 修正 Playwright 單輪抓取在 Google Maps 虛擬化評論列表上的抽卡邏輯，改用可見 card handles 快照避免 stale locator timeout。
* [09:48] 完成 `Rebirth`、`雪可屋`、`羊跳蚤` 三家店的半自動 Playwright 抓取與 SQLite 匯入，作業所需三店資料已齊。
* [10:05] 修正 SQLite 匯入與 summary 查詢：匯入時優先以 `review_id` 比對既有評論，summary 不再因 join 造成評論數誤倍增。
* [10:35] 新增 `src/google_place_review/analysis/` 分析模組與單店 / 跨店分析腳本，涵蓋 preprocessing、sentiment、aspect、lexical、TF-IDF 與圖表輸出骨架。
* [10:45] 執行三家店單店分析與跨店分析，將第一版 CSV / JSON / 圖表輸出寫入 `data/processed/analysis/`。
* [14:20] 補強 `REPORT.md` 中對第一版 sentiment、aspect、lexical 與跨店比較方法的說明，並加入目前已產出的初步結果與輸出位置。
* [14:22] 將 Streamlit 的 `Opinion Analysis` placeholder 改為 `單店意見分析` 與 `跨店意見分析` 兩個子分頁，直接讀取既有 analysis 輸出檔展示結果。
* [14:24] 在 `app_queries.py` 新增 analysis 輸出讀取 helper，讓前端可以穩定讀取單店與跨店的 CSV / PNG，而不需要在頁面中重跑完整 pipeline。
* [15:35] 在 analysis preprocessing 中新增 `每人消費金額` 抽取欄位，並將平均消費金額與消費樣本數納入單店 / 跨店 summary 輸出。
* [15:36] 升級 lexical analysis，新增 `user_dict.txt` 與 `lexical_collocations.csv`，支援關鍵詞前後十字語境的簡化 sentiment 分析。
* [15:38] 重構 Streamlit 版面：將店家搜尋與摘要提升為全頁共用 header、調整 sidebar 文案與店名顯示、在單店 / 跨店分析頁加入平均消費金額、collocation 與橫向正負比例圖。
* [16:05] 進一步調整前端分析視圖：將 sentiment 顯示標準化為 `/10`、把星數分布改為五色堆疊圖、將跨店特色詞改為三欄並排顯示，並新增共同面向的分組長條圖。
* [16:40] 補強 opinion analysis 輸出，新增 `aspect_mentions_enriched.csv`、跨店 `yearly_metrics.csv` 與 collocation 正負向代表原文欄位。
* [16:42] 更新 `user_dict.txt` 與 lexical 過濾，修正 `餐點/類型`、`服務/內用` 等斷詞結果，降低模板化詞彙干擾。
* [16:46] 重寫 Streamlit analysis 頁面，加入可點選 aspect 圖後查看原始評論的 sanity check 區塊、3x2 共同面向小圖與跨店時間趨勢切換。
* [17:35] 重整 aspect 詞典與句段規則法，將環境拆為功能用途並收斂出餐速度關鍵詞，避免快/慢/速度等泛詞誤判。
* [17:37] 在 preprocessing 新增 Google 內建餐點/服務/氣氛評分抽取，並補進單店與跨店 summary 輸出。
* [17:41] 重寫 Streamlit Opinion Analysis 區塊，改為單層分析模式切換、資料表預設收合、點圖後固定高度原文 sanity check 視窗與 diverging ratio chart。
## 2026-03-26
* [18:20] 調整 Opinion Analysis 前端呈現：共同面向改為 3x2 小圖、點圖後原文區塊改為固定高度可捲動視窗、單店摘要指標改為 2x4 排列、跨店情緒圖改為 100% 堆疊橫條圖。
* [18:28] 重寫 `REPORT.md` 為正式交作業版，改成摘要/資料來源/資料庫/前端/分析方法與結果/警訊判讀/商業化服務/附錄八段結構，並補上 Streamlit Community Cloud 部署路線。
* [19:05] 新增 cross-store correlation / regression 分析，輸出 `aspect_star_relation.csv`、`regression_summary.csv`、`regression_coefficients.csv` 與對應圖表。
* [19:12] 將現有分析圖與 regression 結果編號整合進 `REPORT.md`，補上圖號、圖題、表格與正文引用。

* [20:12] 新增 untime.txt 並將 Streamlit 部署環境固定為 Python 3.12，避免 Streamlit Cloud 使用 Python 3.14 與 Altair 相依組合造成匯入失敗。
* [20:13] 新增 runtime.txt 並將 Streamlit 部署環境固定為 Python 3.12，避免 Streamlit Cloud 使用 Python 3.14 與 Altair 相依組合造成匯入失敗。
