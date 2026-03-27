# 專案規則

本專案以 Python 實作 Google Place Review 資料蒐集、資料庫整理、基礎檢視介面與後續 opinion analysis 流程。

## 目前階段範圍

- 本階段已完成三家店的 Google Place reviews 蒐集與 SQLite 匯入。
- 本階段允許維護 Selenium 主線、Playwright 半自動抓取、SQLite、Streamlit 與分析 pipeline。
- 本階段以「先把資料管線與第一版分析跑通」為主，不追求重模型或完整商業化系統。
- 本階段允許建立：
  - 分析模組
  - 分析腳本
  - 分析輸出檔
  - 供前端後續串接的中間結果

## 文件與程式撰寫原則

- `AGENTS.md`、`LOG.md`、`REPORT.md` 以中文撰寫。
- `README.md` 與程式註解盡量使用中文。
- 資料欄位設計需預留未來多地點、SQLite 匯入、增量更新與時間分析需求。
- 爬蟲策略以慢速、保守、易於 debug 為優先。
- 分析模組需優先採模組化設計，避免把邏輯全部塞進 notebook 或單一腳本。

## Logging Rule

- `LOG.md` 用來 append 專案中較大的變動與執行情形。
- `REPORT.md` 用來記錄方法、限制、結果、風險、後續規劃與分析摘要。

### LOG.md 的用途

`LOG.md` 只記錄「做了什麼變更」，例如：

- 策略切換
- 新增或刪除檔案
- 資料夾結構變更
- 核心流程變更
- 核心資料欄位變更
- 實際執行過的驗證路線
- 其他會影響專案方向的重要變動

每條內容都要寫成「人類可讀的變更摘要」，例如：

- `新增 Playwright 半自動抓取流程`
- `建立 SQLite 匯入與查詢層`
- `建立單店與跨店分析腳本`

### LOG.md 禁止內容

`LOG.md` 不應寫入以下內容：

- 原始錯誤訊息全文
- traceback
- 亂碼文字
- 長篇 debug 推理
- selector hit 原始 dump
- terminal 逐行輸出
- 應存在 debug JSON、screenshot、run log 的細節

### LOG.md 格式

建議格式如下：

```md
## 2026-03-06
* [HH:MM] activity
* [HH:MM] activity
```

`LOG.md` 採 append-only；若既有內容已經充滿亂碼或錯誤輸出，允許在整理文件時重寫成乾淨的 changelog。

## REPORT.md 規則

`REPORT.md` 用於記錄：

- 本階段方法
- 限制
- 風險
- 後續規劃
- 資料分析結果
- 作業要求的完成面向摘要

`REPORT.md` 可以順手更新，但請遵守以下原則：

- 以白話文撰寫，讓第一次閱讀的人也能快速理解目前狀況。
- 優先按照「限制或問題 -> 已嘗試的方法 -> 各方法結果差異 -> 目前判斷」整理。
- 若同一限制在不同階段重複出現，應整理在同一區塊內比較，不要分散重寫。
- 可以整理成人類可讀的 debug 摘要，但不要直接貼 traceback、逐輪 debug dump、selector 原始輸出或亂碼內容。
- 若只是本輪 debug 失敗、瀏覽器崩潰、selector 沒命中，細節應寫入 debug artifact，不應直接塞進 `REPORT.md` 正文。
- `REPORT.md` 最上方應保留作業完整面向摘要，並說明目前各面向如何達成。

## 紀錄分流規則

- `LOG.md`
  - 記變更摘要
- `REPORT.md`
  - 記方法、限制、風險、後續規劃、資料結果分析、作業摘要
- `data/raw/.../debug_*.json`
  - 記 Selenium run 的 debug 細節
- `data/playwright/...`
  - 記 Playwright run / probe 的 debug 細節
- `data/processed/analysis/...`
  - 記分析輸出與圖表
- terminal / run output
  - 不直接抄進 `LOG.md`

## 本階段輸出要求

- Selenium 爬蟲保留 headless / non-headless 選項。
- Playwright 可作為半自動抓取與驗證樣本。
- 原始資料保留於 `data/raw/` 與 `data/playwright/`，主格式採 `jsonl`。
- SQLite 作為目前主要查詢與分析層。
- Streamlit 提供基礎資料檢視與時間分布圖表。
- 分析 pipeline 需支援：
  - 單店 sentiment / aspect / lexical analysis
  - 跨店 sentiment / aspect / TF-IDF 比較

## 現在最常用的檔案位置

- Selenium 主資料：`data/raw/`
- Playwright 單輪資料：`data/playwright/<place_id>/runs/`
- SQLite：`data/processed/google_place_reviews.db`
- 分析輸出：`data/processed/analysis/`
- Streamlit app：`app.py`
- 分析模組：`src/google_place_review/analysis/`
