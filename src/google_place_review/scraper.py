from __future__ import annotations

import hashlib
import json
import random
import re
import time
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException, WebDriverException
from selenium.webdriver import ActionChains, ChromeOptions
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement

from google_place_review.config import ScraperConfig
from google_place_review.models import ReviewRecord, ScrapeResult
from google_place_review.storage import ensure_place_paths, load_existing_identities, upsert_latest_reviews, write_jsonl, write_meta


class GooglePlaceReviewScraper:
    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        self.scrape_run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.debug_info: dict[str, Any] = {
            "scrape_run_id": self.scrape_run_id,
            "target_url": config.target_url,
            "headless": config.headless,
            "scroll_strategy": config.scroll_strategy,
            "card_selector_mode": "top_level_data_review_id",
            "panel_interaction_mode": config.panel_interaction_mode,
            "panel_focus_before_scroll": config.panel_focus_before_scroll,
            "events": [],
            "review_entry_candidates": [],
            "round_logs": [],
            "scroll_container_candidates": [],
        }

    def run(self) -> ScrapeResult:
        driver = self._build_driver()
        place_id: str | None = None
        paths: dict[str, Path] | None = None
        try:
            driver.set_page_load_timeout(self.config.page_load_timeout_seconds)
            driver.get(self.config.target_url)
            self._sleep_action()

            source_url = self._resolve_source_url(driver)
            page_title = driver.title
            place_name = self._extract_place_name(driver) or self._extract_place_name_from_title(page_title)
            google_place_token = self._extract_google_place_token(source_url)
            place_id = self._derive_place_id(source_url=source_url, place_name=place_name)
            paths = ensure_place_paths(Path(self.config.output_dir), place_id)
            existing_identities = load_existing_identities(paths["latest_jsonl"])

            self.debug_info.update(
                {
                    "final_url": source_url,
                    "page_title": page_title,
                    "place_name_detected": place_name,
                    "place_id": place_id,
                    "google_place_token": google_place_token,
                    "existing_review_key_count": len(existing_identities),
                }
            )
            self._debug_print(f"final_url: {source_url}")
            self._debug_print(f"page_title: {page_title}")
            self._debug_print(f"place_name_detected: {place_name}")
            self._debug_print(f"place_id: {place_id}")

            reviews_panel = self._open_reviews_panel(driver)
            reviews = self._collect_reviews(
                driver=driver,
                reviews_panel=reviews_panel,
                place_id=place_id,
                place_name=place_name,
                source_url=source_url,
                existing_identities=existing_identities,
            )

            self._save_debug_artifacts(driver, paths, reason="success")

            run_output_path = paths["runs_dir"] / f"{self.scrape_run_id}.jsonl"
            write_jsonl(run_output_path, reviews)

            new_reviews_added = upsert_latest_reviews(paths["latest_jsonl"], reviews)

            total_reviews_in_latest = len(load_existing_identities(paths["latest_jsonl"]))
            write_meta(
                paths["meta_json"],
                place_id=place_id,
                place_name=place_name,
                source_url=source_url,
                scrape_run_id=self.scrape_run_id,
                total_reviews_in_latest=total_reviews_in_latest,
                reviews_in_run=len(reviews),
            )

            return ScrapeResult(
                place_id=place_id,
                place_name=place_name,
                scrape_run_id=self.scrape_run_id,
                total_reviews=len(reviews),
                new_reviews_added=new_reviews_added,
                latest_output_path=paths["latest_jsonl"],
                run_output_path=run_output_path,
            )
        except Exception as exc:
            self.debug_info["error"] = repr(exc)
            if paths is not None:
                self._save_debug_artifacts(driver, paths, reason="failure")
            raise
        finally:
            driver.quit()

    def _build_driver(self) -> webdriver.Chrome:
        options = ChromeOptions()
        if self.config.headless:
            options.add_argument("--headless=new")
        options.add_argument("--lang=zh-TW")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1600,2200")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-notifications")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        service = self._build_chrome_service()
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def _build_chrome_service(self) -> Service:
        if self.config.chromedriver_path:
            chromedriver_path = Path(self.config.chromedriver_path).expanduser().resolve()
            if not chromedriver_path.exists():
                raise FileNotFoundError(f"??? chromedriver?{chromedriver_path}")
            self.debug_info["chromedriver_source"] = "explicit_path"
            self.debug_info["chromedriver_path"] = str(chromedriver_path)
            self._debug_print(f"chromedriver_source: explicit_path ({chromedriver_path})")
            return Service(executable_path=str(chromedriver_path))

        self.debug_info["chromedriver_source"] = "selenium_manager"
        self._debug_print("chromedriver_source: selenium_manager")
        return Service()

    def _resolve_source_url(self, driver: webdriver.Chrome) -> str:
        time.sleep(2)
        return driver.current_url

    def _extract_place_name(self, driver: webdriver.Chrome) -> str | None:
        selectors = [
            "h1.DUwDvf",
            "h1.fontHeadlineLarge",
            "[role='main'] h1",
            "div[role='main'] h1 span",
        ]
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                text = self._clean_place_name(element.text)
                if text:
                    return text
        return None

    def _extract_place_name_from_title(self, page_title: str) -> str | None:
        return self._clean_place_name(page_title)

    def _clean_place_name(self, value: str | None) -> str | None:
        if not value:
            return None
        cleaned = unicodedata.normalize("NFKC", value).strip()
        cleaned = re.sub(r"\s*-\s*Google 地圖$", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"\s*-\s*Google Maps$", "", cleaned, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned or None

    def _open_reviews_panel(self, driver: webdriver.Chrome) -> WebElement:
        review_intent_url = "!9m1!1b1" in driver.current_url or "%211b1" in driver.current_url
        initial_wait_seconds = 12 if review_intent_url else 5
        existing_panel = self._locate_reviews_panel(driver, wait_seconds=initial_wait_seconds)
        if existing_panel is not None:
            self.debug_info["reviews_entry_detected"] = True
            self.debug_info["reviews_entry_mode"] = "already_open"
            self._debug_print("reviews_entry_detected: already_open")
            return existing_panel
        candidates = self._discover_review_entry_candidates(driver)
        self.debug_info["review_entry_candidates"] = [self._candidate_for_log(item) for item in candidates]
        self._debug_print(f"reviews_entry_detected: {bool(candidates)}")
        self._debug_print(f"review_entry_candidate_count: {len(candidates)}")
        for index, candidate in enumerate(self.debug_info["review_entry_candidates"], start=1):
            self._debug_print(
                f"candidate[{index}]: source={candidate['source']} score={candidate['score']} tag={candidate['tag_name']} role={candidate['role']} aria={candidate['aria_label']} text={candidate['text']}"
            )

        for candidate in candidates:
            if self._candidate_looks_like_write_review(candidate):
                continue
            if not self._click_review_candidate(driver, candidate["element"]):
                continue
            self._sleep_action()
            if self._write_review_modal_is_open(driver):
                self._record_event("clicked_write_review_modal_instead")
                self._close_write_review_modal(driver)
                continue
            panel = self._locate_reviews_panel(driver, wait_seconds=12)
            if panel is not None:
                self.debug_info["reviews_entry_detected"] = True
                self.debug_info["reviews_entry_mode"] = f"clicked:{candidate['source']}"
                self._debug_print(f"reviews_entry_opened_by: {candidate['source']}")
                return panel

        self.debug_info["reviews_entry_detected"] = False
        raise RuntimeError("找不到可點擊的 reviews 入口或無法成功進入 reviews 區。")

    def _locate_reviews_panel(self, driver: webdriver.Chrome, wait_seconds: int) -> WebElement | None:
        selectors = [
            "div[role='feed']",
            "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde",
            "div.m6QErb[aria-label*='評論']",
            "div.m6QErb[aria-label*='reviews']",
        ]
        deadline = time.time() + wait_seconds
        best_panel: WebElement | None = None
        best_card_count = 0
        while time.time() < deadline:
            for selector in selectors:
                panels = driver.find_elements(By.CSS_SELECTOR, selector)
                for panel in panels:
                    try:
                        cards = self._find_review_cards(panel)
                        card_count = len(cards)
                        panel_text = (panel.get_attribute("aria-label") or "") + " " + (panel.text[:300] if panel.text else "")
                    except StaleElementReferenceException:
                        continue
                    if card_count > best_card_count:
                        best_panel = panel
                        best_card_count = card_count
                    if card_count > 0:
                        self._record_event(f"located_reviews_panel_with_cards:{selector}:{card_count}")
                        return panel
                    if re.search(r"排序|最相關|評論|reviews", panel_text, flags=re.IGNORECASE):
                        best_panel = panel
            time.sleep(1)
        if best_panel is not None:
            self._record_event(f"located_reviews_panel_without_cards:{best_card_count}")
        return best_panel

    def _discover_review_entry_candidates(self, driver: webdriver.Chrome) -> list[dict[str, Any]]:
        candidate_specs = [
            (
                "aria_review_button",
                By.XPATH,
                "//button[contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(@aria-label, 'Google reviews') or contains(@aria-label, '則評論')]",
            ),
            (
                "text_review_button",
                By.XPATH,
                "//button[contains(normalize-space(.), '評論') or contains(normalize-space(.), 'reviews') or contains(normalize-space(.), '則評論')]",
            ),
            (
                "role_button_review",
                By.XPATH,
                "//*[@role='button' or @role='tab' or self::a][contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(normalize-space(.), '評論') or contains(normalize-space(.), 'reviews')]",
            ),
            (
                "review_count_block",
                By.XPATH,
                "//*[contains(@class, 'F7nice') or contains(@class, 'hh2c6') or contains(@class, 'jANrlb') or contains(@class, 'LBgpqf')]//*[self::button or self::a or @role='button' or @role='tab']",
            ),
            (
                "main_panel_button",
                By.XPATH,
                "//*[@role='main']//*[self::button or self::a or @role='button' or @role='tab'][contains(@aria-label, '評論') or contains(normalize-space(.), '評論') or contains(@aria-label, 'reviews') or contains(normalize-space(.), 'reviews')]",
            ),
        ]

        dedup: dict[str, dict[str, Any]] = {}
        hit_summary: list[dict[str, Any]] = []
        for source, by, locator in candidate_specs:
            try:
                elements = driver.find_elements(by, locator)
            except WebDriverException:
                elements = []
            hit_summary.append({"source": source, "hits": len(elements)})
            for element in elements:
                metadata = self._build_candidate_metadata(source, element)
                if metadata is None:
                    continue
                dedup.setdefault(metadata["signature"], metadata)

        self.debug_info["review_entry_selector_hits"] = hit_summary
        for item in hit_summary:
            self._debug_print(f"selector_hit: {item['source']}={item['hits']}")
        return sorted(dedup.values(), key=lambda item: item["score"], reverse=True)[:30]

    def _build_candidate_metadata(self, source: str, element: WebElement) -> dict[str, Any] | None:
        try:
            text = (element.text or "").strip()
            aria_label = (element.get_attribute("aria-label") or "").strip()
            role = (element.get_attribute("role") or "").strip()
            class_name = (element.get_attribute("class") or "").strip()
            tag_name = element.tag_name
        except StaleElementReferenceException:
            return None

        combined = f"{aria_label} {text}".strip()
        if not combined:
            return None
        has_review_signal = bool(re.search(r"???|??|reviews|google reviews", combined, flags=re.IGNORECASE))
        if not has_review_signal:
            return None

        score = 0
        if re.search(r"則評論|[0-9,]+\s*reviews", combined, flags=re.IGNORECASE):
            score += 6
        if re.search(r"評論|reviews|google reviews", combined, flags=re.IGNORECASE):
            score += 4
        if role in {"button", "tab"} or tag_name in {"button", "a"}:
            score += 2
        if any(token in class_name for token in ["F7nice", "hh2c6", "LBgpqf"]):
            score += 2
        if self._candidate_looks_like_write_review({"text": text, "aria_label": aria_label}):
            score -= 10
        if score <= 0:
            return None

        signature = "||".join([source, tag_name, role, aria_label, text])
        return {
            "element": element,
            "source": source,
            "text": text,
            "aria_label": aria_label,
            "role": role,
            "class_name": class_name,
            "tag_name": tag_name,
            "score": score,
            "signature": signature,
        }

    def _candidate_for_log(self, candidate: dict[str, Any]) -> dict[str, Any]:
        return {
            "source": candidate["source"],
            "text": candidate["text"],
            "aria_label": candidate["aria_label"],
            "role": candidate["role"],
            "tag_name": candidate["tag_name"],
            "score": candidate["score"],
        }

    def _candidate_looks_like_write_review(self, candidate: dict[str, Any]) -> bool:
        text = f"{candidate.get('aria_label', '')} {candidate.get('text', '')}"
        return bool(re.search(r"撰寫評論|寫評論|write a review", text, flags=re.IGNORECASE))

    def _click_review_candidate(self, driver: webdriver.Chrome, element: WebElement) -> bool:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", element)
            self._record_event("clicked_review_candidate")
            return True
        except WebDriverException:
            return False

    def _write_review_modal_is_open(self, driver: webdriver.Chrome) -> bool:
        modals = driver.find_elements(By.CSS_SELECTOR, "iframe[aria-label*='撰寫評論'], iframe[aria-label*='Write a review']")
        return bool(modals)

    def _close_write_review_modal(self, driver: webdriver.Chrome) -> None:
        close_targets = driver.find_elements(By.XPATH, "//button[contains(@aria-label, '關閉') or contains(@aria-label, 'Close')]")
        for button in close_targets:
            try:
                driver.execute_script("arguments[0].click();", button)
                self._sleep_action()
                return
            except WebDriverException:
                continue

    def _locate_reviews_panel(self, driver: webdriver.Chrome, wait_seconds: int) -> WebElement | None:
        selector_specs = [
            ("verified_selector", "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde"),
            ("feed_fallback", "div[role='feed']"),
            ("m6QErb_fallback", "div.m6QErb"),
        ]
        deadline = time.time() + wait_seconds
        best_panel: WebElement | None = None
        best_card_count = 0
        while time.time() < deadline:
            for detection_mode, selector in selector_specs:
                try:
                    panels = driver.find_elements(By.CSS_SELECTOR, selector)
                except WebDriverException:
                    panels = []
                for panel in panels:
                    try:
                        metrics = self._get_scroll_metrics(driver, panel)
                        overflow_y = driver.execute_script("return window.getComputedStyle(arguments[0]).overflowY;", panel) or ""
                        cards = self._find_review_cards(panel)
                        card_count = len(cards)
                        scrollable = overflow_y in {"auto", "scroll"} and metrics.get("scroll_height", 0) > metrics.get("client_height", 0) + 40
                    except (StaleElementReferenceException, WebDriverException):
                        continue
                    self.debug_info["panel_selector_hit"] = selector
                    self.debug_info["panel_detection_mode"] = detection_mode
                    self.debug_info["panel_scrollable_verified"] = scrollable
                    self.debug_info["panel_review_count"] = card_count
                    if card_count > best_card_count:
                        best_panel = panel
                        best_card_count = card_count
                    if scrollable and card_count > 0:
                        self._record_event(f"located_reviews_panel_with_cards:{selector}:{card_count}")
                        return panel
                    if scrollable or card_count > 0:
                        best_panel = panel
            time.sleep(1)
        if best_panel is not None:
            self._record_event(f"located_reviews_panel_without_cards:{best_card_count}")
        return best_panel

    def _discover_review_entry_candidates(self, driver: webdriver.Chrome) -> list[dict[str, Any]]:
        candidate_specs = [
            (
                "hh2c6_review_tab_aria",
                By.CSS_SELECTOR,
                "button.hh2c6[role='tab'][aria-label*='評論'], button.hh2c6[role='tab'][aria-label*='reviews'], button.hh2c6[role='tab'][aria-label*='Reviews']",
            ),
            (
                "hh2c6_any_tab",
                By.CSS_SELECTOR,
                "button.hh2c6[role='tab']",
            ),
            (
                "generic_review_tab",
                By.XPATH,
                "//*[@role='tab'][contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(@aria-label, 'Reviews')]",
            ),
            (
                "aria_review_button",
                By.XPATH,
                "//button[contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(@aria-label, 'Reviews') or contains(@aria-label, 'Google reviews')]",
            ),
            (
                "text_review_button",
                By.XPATH,
                "//button[contains(normalize-space(.), '評論') or contains(normalize-space(.), 'reviews') or contains(normalize-space(.), 'Reviews')]",
            ),
            (
                "role_button_review",
                By.XPATH,
                "//*[@role='button' or @role='tab' or self::a][contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(@aria-label, 'Reviews') or contains(normalize-space(.), '評論') or contains(normalize-space(.), 'reviews') or contains(normalize-space(.), 'Reviews')]",
            ),
            (
                "review_count_block",
                By.XPATH,
                "//*[contains(@class, 'F7nice') or contains(@class, 'hh2c6') or contains(@class, 'jANrlb') or contains(@class, 'LBgpqf')]//*[self::button or self::a or @role='button' or @role='tab']",
            ),
            (
                "main_panel_button",
                By.XPATH,
                "//*[@role='main']//*[self::button or self::a or @role='button' or @role='tab'][contains(@aria-label, '評論') or contains(normalize-space(.), '評論') or contains(@aria-label, 'reviews') or contains(normalize-space(.), 'reviews') or contains(@aria-label, 'Reviews') or contains(normalize-space(.), 'Reviews')]",
            ),
        ]

        dedup: dict[str, dict[str, Any]] = {}
        hit_summary: list[dict[str, Any]] = []
        for source, by, locator in candidate_specs:
            try:
                elements = driver.find_elements(by, locator)
            except WebDriverException:
                elements = []
            hit_summary.append({"source": source, "hits": len(elements)})
            for element in elements:
                metadata = self._build_candidate_metadata(source, element)
                if metadata is None:
                    metadata = self._build_candidate_metadata_fallback(source, element)
                if metadata is None:
                    continue
                dedup.setdefault(metadata["signature"], metadata)

        self.debug_info["review_entry_selector_hits"] = hit_summary
        for item in hit_summary:
            self._debug_print(f"selector_hit: {item['source']}={item['hits']}")
        return sorted(dedup.values(), key=lambda item: item["score"], reverse=True)[:30]

    def _build_candidate_metadata(self, source: str, element: WebElement) -> dict[str, Any] | None:
        try:
            text = (element.text or "").strip()
            aria_label = (element.get_attribute("aria-label") or "").strip()
            role = (element.get_attribute("role") or "").strip()
            class_name = (element.get_attribute("class") or "").strip()
            data_tab_index = (element.get_attribute("data-tab-index") or "").strip()
            aria_selected = (element.get_attribute("aria-selected") or "").strip()
            tag_name = element.tag_name
        except StaleElementReferenceException:
            return None

        combined = f"{aria_label} {text}".strip()
        if not combined:
            return None
        normalized = combined.lower()
        is_reviews_index_tab = "hh2c6" in class_name and role == "tab" and data_tab_index == "2"
        has_review_signal = (
            "評論" in combined
            or "review" in normalized
            or "google reviews" in normalized
            or is_reviews_index_tab
        )
        if not has_review_signal:
            return None

        score = 0
        if role == "tab":
            score += 6
        if "hh2c6" in class_name:
            score += 6
        if data_tab_index == "2":
            score += 4
        if aria_selected == "true":
            score += 2
        if "對「" in aria_label and "評論" in aria_label:
            score += 8
        if "評論" in combined:
            score += 6
        if "review" in normalized:
            score += 6
        if is_reviews_index_tab:
            score += 6
        if role in {"button", "tab"} or tag_name in {"button", "a"}:
            score += 2
        if any(token in class_name for token in ["F7nice", "hh2c6", "LBgpqf"]):
            score += 2
        if self._candidate_looks_like_write_review({"text": text, "aria_label": aria_label}):
            score -= 10
        if score <= 0:
            return None

        signature = "||".join([source, tag_name, role, aria_label, text])
        return {
            "element": element,
            "source": source,
            "text": text,
            "aria_label": aria_label,
            "role": role,
            "class_name": class_name,
            "tag_name": tag_name,
            "score": score,
            "signature": signature,
        }

    def _candidate_looks_like_write_review(self, candidate: dict[str, Any]) -> bool:
        text = f"{candidate.get('aria_label', '')} {candidate.get('text', '')}"
        lowered = text.lower()
        return "撰寫評論" in text or "寫評論" in text or "write a review" in lowered

    def _write_review_modal_is_open(self, driver: webdriver.Chrome) -> bool:
        modals = driver.find_elements(By.CSS_SELECTOR, "iframe[aria-label*='撰寫評論'], iframe[aria-label*='Write a review']")
        return bool(modals)

    def _open_reviews_panel(self, driver: webdriver.Chrome) -> WebElement:
        review_intent_url = "!9m1!1b1" in driver.current_url or "%211b1" in driver.current_url
        initial_wait_seconds = 15 if review_intent_url else 5
        existing_panel = self._locate_reviews_panel(driver, wait_seconds=initial_wait_seconds)
        if existing_panel is not None:
            existing_card_count = len(self._find_review_cards(existing_panel))
            self.debug_info["reviews_entry_detected"] = True
            self.debug_info["reviews_entry_mode"] = "review_intent_direct" if review_intent_url else "already_open"
            self.debug_info["entered_reviews_context"] = True
            self.debug_info["entered_full_review_list"] = existing_card_count > 0
            self._debug_print(f"reviews_entry_detected: {self.debug_info['reviews_entry_mode']}")
            return existing_panel

        if review_intent_url:
            self.debug_info["review_intent_first_locate_failed"] = True
            self._debug_print("review_intent_first_locate_failed: True")
            self._sleep_action()
            retry_panel = self._locate_reviews_panel(driver, wait_seconds=8)
            if retry_panel is not None:
                retry_card_count = len(self._find_review_cards(retry_panel))
                self.debug_info["reviews_entry_detected"] = True
                self.debug_info["reviews_entry_mode"] = "review_intent_direct_retry"
                self.debug_info["review_intent_retry_succeeded"] = True
                self.debug_info["entered_reviews_context"] = True
                self.debug_info["entered_full_review_list"] = retry_card_count > 0
                self._debug_print("review_intent_retry_succeeded: True")
                return retry_panel

        candidates = self._discover_review_entry_candidates(driver)
        self.debug_info["review_entry_candidates"] = [self._candidate_for_log(item) for item in candidates]
        self._debug_print(f"reviews_entry_detected: {bool(candidates)}")
        self._debug_print(f"review_entry_candidate_count: {len(candidates)}")
        for index, candidate in enumerate(self.debug_info["review_entry_candidates"], start=1):
            self._debug_print(
                f"candidate[{index}]: source={candidate['source']} score={candidate['score']} tag={candidate['tag_name']} role={candidate['role']} aria={candidate['aria_label']} text={candidate['text']}"
            )

        for candidate in candidates:
            if self._candidate_looks_like_write_review(candidate):
                continue
            self.debug_info["tab_fallback_attempted"] = True
            self.debug_info["tab_fallback_selector"] = candidate["source"]
            if not self._click_review_candidate(driver, candidate["element"]):
                continue
            self._sleep_action()
            if self._write_review_modal_is_open(driver):
                self._record_event("clicked_write_review_modal_instead")
                self._close_write_review_modal(driver)
                continue
            panel = self._locate_reviews_panel(driver, wait_seconds=12)
            if panel is not None:
                panel_card_count = len(self._find_review_cards(panel))
                self.debug_info["reviews_entry_detected"] = True
                self.debug_info["reviews_entry_mode"] = f"clicked:{candidate['source']}"
                self.debug_info["entered_reviews_context"] = True
                self.debug_info["entered_full_review_list"] = panel_card_count > 0
                self._debug_print(f"reviews_entry_opened_by: {candidate['source']}")
                return panel

        self.debug_info["reviews_entry_detected"] = False
        self.debug_info["entered_reviews_context"] = False
        self.debug_info["entered_full_review_list"] = False
        if review_intent_url:
            raise RuntimeError("review_intent_direct_failed")
        raise RuntimeError("unable_to_enter_reviews_context")

    def _locate_reviews_panel(self, driver: webdriver.Chrome, wait_seconds: int) -> WebElement | None:
        selector_specs = [
            ("verified_selector", "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde"),
            ("feed_fallback", "div[role='feed']"),
            ("m6QErb_fallback", "div.m6QErb"),
        ]
        deadline = time.time() + wait_seconds
        best_panel: WebElement | None = None
        best_score = -1
        best_selector = ""
        best_detection_mode = ""
        best_scrollable = False
        best_card_count = 0
        while time.time() < deadline:
            for detection_mode, selector in selector_specs:
                try:
                    panels = driver.find_elements(By.CSS_SELECTOR, selector)
                except WebDriverException:
                    panels = []
                for panel in panels:
                    try:
                        metrics = self._get_scroll_metrics(driver, panel)
                        overflow_y = driver.execute_script("return window.getComputedStyle(arguments[0]).overflowY;", panel) or ""
                        cards = self._find_review_cards(panel)
                        card_count = len(cards)
                        scrollable = overflow_y in {"auto", "scroll"} and metrics.get("scroll_height", 0) > metrics.get("client_height", 0) + 40
                    except (StaleElementReferenceException, WebDriverException):
                        continue
                    self.debug_info["panel_selector_hit"] = selector
                    self.debug_info["panel_detection_mode"] = detection_mode
                    self.debug_info["panel_scrollable_verified"] = scrollable
                    self.debug_info["panel_review_count"] = card_count
                    if scrollable and card_count > 0:
                        self._record_event(f"located_reviews_panel_with_cards:{selector}:{card_count}")
                        return panel
                    score = 0
                    if detection_mode == "verified_selector":
                        score += 10
                    elif detection_mode == "feed_fallback":
                        score += 4
                    if scrollable:
                        score += 6
                    if card_count > 0:
                        score += 3
                    if score > best_score:
                        best_panel = panel
                        best_score = score
                        best_selector = selector
                        best_detection_mode = detection_mode
                        best_scrollable = scrollable
                        best_card_count = card_count
            time.sleep(1)
        if best_panel is not None and best_scrollable:
            self.debug_info["panel_selector_hit"] = best_selector
            self.debug_info["panel_detection_mode"] = best_detection_mode
            self.debug_info["panel_scrollable_verified"] = best_scrollable
            self.debug_info["panel_review_count"] = best_card_count
            self._record_event(f"located_reviews_panel_without_cards:{best_selector}:{best_card_count}")
            return best_panel
        return None

    def _discover_review_entry_candidates(self, driver: webdriver.Chrome) -> list[dict[str, Any]]:
        candidate_specs = [
            (
                "hh2c6_review_tab_aria",
                By.CSS_SELECTOR,
                "button.hh2c6[role='tab'][aria-label*='評論'], button.hh2c6[role='tab'][aria-label*='reviews'], button.hh2c6[role='tab'][aria-label*='Reviews']",
            ),
            (
                "hh2c6_any_tab",
                By.CSS_SELECTOR,
                "button.hh2c6[role='tab']",
            ),
            (
                "generic_review_tab",
                By.XPATH,
                "//*[@role='tab'][contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(@aria-label, 'Reviews')]",
            ),
            (
                "aria_review_button",
                By.XPATH,
                "//button[contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(@aria-label, 'Reviews') or contains(@aria-label, 'Google reviews')]",
            ),
            (
                "text_review_button",
                By.XPATH,
                "//button[contains(normalize-space(.), '評論') or contains(normalize-space(.), 'reviews') or contains(normalize-space(.), 'Reviews')]",
            ),
            (
                "role_button_review",
                By.XPATH,
                "//*[@role='button' or @role='tab' or self::a][contains(@aria-label, '評論') or contains(@aria-label, 'reviews') or contains(@aria-label, 'Reviews') or contains(normalize-space(.), '評論') or contains(normalize-space(.), 'reviews') or contains(normalize-space(.), 'Reviews')]",
            ),
            (
                "review_count_block",
                By.XPATH,
                "//*[contains(@class, 'F7nice') or contains(@class, 'hh2c6') or contains(@class, 'jANrlb') or contains(@class, 'LBgpqf')]//*[self::button or self::a or @role='button' or @role='tab']",
            ),
            (
                "main_panel_button",
                By.XPATH,
                "//*[@role='main']//*[self::button or self::a or @role='button' or @role='tab'][contains(@aria-label, '評論') or contains(normalize-space(.), '評論') or contains(@aria-label, 'reviews') or contains(normalize-space(.), 'reviews') or contains(@aria-label, 'Reviews') or contains(normalize-space(.), 'Reviews')]",
            ),
        ]

        dedup: dict[str, dict[str, Any]] = {}
        hit_summary: list[dict[str, Any]] = []
        for source, by, locator in candidate_specs:
            try:
                elements = driver.find_elements(by, locator)
            except WebDriverException:
                elements = []
            hit_summary.append({"source": source, "hits": len(elements)})
            for element in elements:
                metadata = self._build_candidate_metadata(source, element)
                if metadata is None:
                    continue
                dedup.setdefault(metadata["signature"], metadata)

        self.debug_info["review_entry_selector_hits"] = hit_summary
        for item in hit_summary:
            self._debug_print(f"selector_hit: {item['source']}={item['hits']}")
        return sorted(dedup.values(), key=lambda item: item["score"], reverse=True)[:30]

    def _build_candidate_metadata(self, source: str, element: WebElement) -> dict[str, Any] | None:
        try:
            text = (element.text or "").strip()
            aria_label = (element.get_attribute("aria-label") or "").strip()
            role = (element.get_attribute("role") or "").strip()
            class_name = (element.get_attribute("class") or "").strip()
            data_tab_index = (element.get_attribute("data-tab-index") or "").strip()
            aria_selected = (element.get_attribute("aria-selected") or "").strip()
            tag_name = element.tag_name
        except StaleElementReferenceException:
            return None

        combined = f"{aria_label} {text}".strip()
        if not combined:
            return None
        normalized = combined.lower()
        has_review_signal = (
            "評論" in combined
            or "review" in normalized
            or "google reviews" in normalized
        )
        if not has_review_signal:
            return None

        score = 0
        if role == "tab":
            score += 6
        if "hh2c6" in class_name:
            score += 6
        if data_tab_index == "2":
            score += 2
        if aria_selected == "true":
            score += 2
        if ("評論" in aria_label and "對" in aria_label) or ("review" in normalized and "rebirth" in normalized):
            score += 8
        if "評論" in combined:
            score += 6
        if "review" in normalized:
            score += 6
        if role in {"button", "tab"} or tag_name in {"button", "a"}:
            score += 2
        if any(token in class_name for token in ["F7nice", "hh2c6", "LBgpqf"]):
            score += 2
        if self._candidate_looks_like_write_review({"text": text, "aria_label": aria_label}):
            score -= 10
        if score <= 0:
            return None

        signature = "||".join([source, tag_name, role, aria_label, text])
        return {
            "element": element,
            "source": source,
            "text": text,
            "aria_label": aria_label,
            "role": role,
            "class_name": class_name,
            "tag_name": tag_name,
            "score": score,
            "signature": signature,
        }

    def _candidate_looks_like_write_review(self, candidate: dict[str, Any]) -> bool:
        text = f"{candidate.get('aria_label', '')} {candidate.get('text', '')}"
        lowered = text.lower()
        return "撰寫評論" in text or "寫評論" in text or "write a review" in lowered

    def _write_review_modal_is_open(self, driver: webdriver.Chrome) -> bool:
        modals = driver.find_elements(By.CSS_SELECTOR, "iframe[aria-label*='撰寫評論'], iframe[aria-label*='Write a review']")
        return bool(modals)

    def _build_candidate_metadata_fallback(self, source: str, element: WebElement) -> dict[str, Any] | None:
        try:
            text = (element.text or "").strip()
            aria_label = (element.get_attribute("aria-label") or "").strip()
            role = (element.get_attribute("role") or "").strip()
            class_name = (element.get_attribute("class") or "").strip()
            data_tab_index = (element.get_attribute("data-tab-index") or "").strip()
            aria_selected = (element.get_attribute("aria-selected") or "").strip()
            tag_name = element.tag_name
        except StaleElementReferenceException:
            return None

        combined = f"{aria_label} {text}".strip()
        normalized = combined.lower()
        review_words = ["review", "reviews", "google reviews"]
        has_review_word = any(word in combined for word in ["評論", "评论"]) or any(
            word in normalized for word in review_words
        )
        generic_tab_signal = ("hh2c6" in class_name and role == "tab") or data_tab_index == "2"
        if not combined and not generic_tab_signal:
            return None
        if not has_review_word and not generic_tab_signal:
            return None

        score = 0
        if role == "tab":
            score += 6
        if "hh2c6" in class_name:
            score += 4
        if data_tab_index == "2":
            score += 4
        if aria_selected == "true":
            score += 2
        if ("評論" in aria_label) or ("评论" in aria_label) or ("review" in normalized and "rebirth" in normalized):
            score += 8
        if has_review_word:
            score += 6
        if "hh2c6" in class_name and role == "tab":
            score += 2
        if role in {"button", "tab"} or tag_name in {"button", "a"}:
            score += 2
        if any(token in class_name for token in ["F7nice", "hh2c6", "LBgpqf"]):
            score += 2
        if self._candidate_looks_like_write_review({"text": text, "aria_label": aria_label}):
            score -= 10
        if score <= 0:
            return None

        signature = "||".join([source, tag_name, role, aria_label, text, data_tab_index])
        return {
            "element": element,
            "source": source,
            "text": text,
            "aria_label": aria_label,
            "role": role,
            "class_name": class_name,
            "tag_name": tag_name,
            "score": score,
            "signature": signature,
        }

    def _collect_reviews(
        self,
        *,
        driver: webdriver.Chrome,
        reviews_panel: WebElement,
        place_id: str,
        place_name: str | None,
        source_url: str,
        existing_identities: set[str],
    ) -> list[ReviewRecord]:
        scroll_container = self._resolve_scroll_container(driver, reviews_panel)
        container_signature = self._container_signature(scroll_container)
        interaction_probe = self._probe_panel_interaction(driver, scroll_container)
        container_summary = self._build_container_candidate_summary(driver, scroll_container)
        review_view_mode = self._determine_review_view_mode(container_summary)
        container_summary.update(
            {
                "interaction_verified": interaction_probe["interaction_verified"],
                "panel_focus_succeeded": interaction_probe["panel_focus_succeeded"],
                "panel_scroll_response_detected": interaction_probe["panel_scroll_response_detected"],
                "outer_page_scroll_detected": interaction_probe["outer_page_scroll_detected"],
                "map_view_interference_detected": interaction_probe["map_view_interference_detected"],
                "selection_mode": container_summary.get("selection_mode", "fallback_candidates"),
            }
        )
        self.debug_info["verified_scroll_selector"] = "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde"
        self.debug_info["review_view_mode"] = review_view_mode
        self.debug_info["selected_scroll_container"] = container_summary
        self.debug_info["entered_reviews_context"] = True
        if review_view_mode == "full_review_list":
            self.debug_info["entry_verdict"] = "entered_full_review_list"
            self.debug_info["entered_full_review_list"] = True
        elif review_view_mode == "embedded_review_section":
            self.debug_info["entry_verdict"] = "entered_embedded_review_section"
            self.debug_info["entered_full_review_list"] = False
            self._debug_print("entry_verdict: entered_embedded_review_section")
            self._expand_visible_review_bodies(driver, scroll_container)
            embedded_cards = self._find_review_cards(scroll_container)
            observer_key = self._prepare_container_observer(driver, scroll_container)
            promotion_scroll = self._scroll_reviews_panel(
                driver,
                scroll_container,
                last_card=embedded_cards[-1] if embedded_cards else None,
            )
            promotion_wait = self._wait_for_review_batch(
                driver=driver,
                reviews_panel=scroll_container,
                previous_visible_count=len(embedded_cards),
                previous_scroll_height=container_summary.get("scroll_height", -1),
                previous_last_card_signature=self._review_card_signature(embedded_cards[-1]) if embedded_cards else "",
                known_review_ids_before=self._review_ids_in_container(scroll_container),
                observer_key=observer_key,
            )
            scroll_container = self._resolve_scroll_container(driver, scroll_container)
            container_signature = self._container_signature(scroll_container)
            container_summary = self._build_container_candidate_summary(driver, scroll_container)
            review_view_mode = self._determine_review_view_mode(container_summary)
            container_summary.update(
                {
                    "interaction_verified": promotion_scroll.get("interaction_verified", interaction_probe["interaction_verified"]),
                    "panel_focus_succeeded": promotion_scroll.get("panel_focus_succeeded", interaction_probe["panel_focus_succeeded"]),
                    "panel_scroll_response_detected": promotion_scroll.get("panel_scroll_response_detected", interaction_probe["panel_scroll_response_detected"]),
                    "outer_page_scroll_detected": promotion_scroll.get("outer_page_scroll_detected", interaction_probe["outer_page_scroll_detected"]),
                    "map_view_interference_detected": promotion_scroll.get("map_view_interference_detected", interaction_probe["map_view_interference_detected"]),
                    "selection_mode": container_summary.get("selection_mode", "fallback_candidates"),
                }
            )
            self.debug_info["selected_scroll_container"] = container_summary
            self.debug_info["review_view_mode"] = review_view_mode
            self.debug_info["embedded_promotion_attempt"] = {
                "strategy": promotion_scroll.get("lazy_load_strategy"),
                "scroll_delta": promotion_scroll.get("scroll_delta", 0),
                "wait_result": promotion_wait.get("reason"),
                "wait_duration_seconds": promotion_wait.get("wait_duration_seconds", 0.0),
                "post_promotion_review_view_mode": review_view_mode,
            }
            if review_view_mode == "full_review_list":
                self.debug_info["entry_verdict"] = "entered_full_review_list"
                self.debug_info["entered_full_review_list"] = True
                self._debug_print("embedded_review_section_promoted_to_full_review_list: True")
            else:
                self.debug_info["final_stop_reason"] = "not_in_full_review_list"
                self.debug_info["final_scroll_metrics"] = self._get_scroll_metrics(driver, scroll_container)
                self.debug_info["strategy_verdict"] = self._determine_strategy_verdict()
                self.debug_info["scrape_verdict"] = "no_reviews_captured"
                self._debug_print("stop_reason: not_in_full_review_list")
                return []
        else:
            self.debug_info["entry_verdict"] = "failed_to_enter_reviews_context"
            self.debug_info["entered_full_review_list"] = False
            self.debug_info["final_stop_reason"] = "not_in_full_review_list"
            self.debug_info["final_scroll_metrics"] = self._get_scroll_metrics(driver, scroll_container)
            self.debug_info["strategy_verdict"] = self._determine_strategy_verdict()
            self.debug_info["scrape_verdict"] = "no_reviews_captured"
            self._debug_print("stop_reason: not_in_full_review_list")
            return []

        seen_identities: set[str] = set()
        records: list[ReviewRecord] = []
        idle_rounds = 0
        stagnant_scroll_rounds = 0
        previous_scroll_top = -1
        consecutive_existing_rounds = 0
        stop_reason = "max_rounds_reached"
        last_round_metrics = self._get_scroll_metrics(driver, scroll_container)
        existing_stop_threshold = max(5, self.config.max_idle_rounds)
        self.debug_info["_lazy_strategy_index"] = 0
        for round_index in range(1, self.config.max_scroll_rounds + 1):
            scroll_container = self._resolve_scroll_container(driver, scroll_container)
            container_signature = self._container_signature(scroll_container)
            self._expand_visible_review_bodies(driver, scroll_container)
            cards_before_collect = self._find_review_cards(scroll_container)
            visible_count = len(cards_before_collect)
            known_review_ids_before = self._review_ids_in_container(scroll_container)
            last_card_signature_before_collect = self._review_card_signature(cards_before_collect[-1]) if cards_before_collect else ""
            last_review_id_before_collect = self._extract_review_id(cards_before_collect[-1]) if cards_before_collect else None
            before_count = len(records)

            for card in cards_before_collect:
                record = self._extract_review_from_card(
                    card=card,
                    place_id=place_id,
                    place_name=place_name,
                    source_url=source_url,
                )
                if not record:
                    continue
                record_identity = self._review_identity(record)
                if record_identity in seen_identities:
                    continue
                seen_identities.add(record_identity)
                records.append(record)
                if self.config.max_reviews and len(records) >= self.config.max_reviews:
                    metrics = self._get_scroll_metrics(driver, scroll_container)
                    self._record_round_log(
                        round_index=round_index,
                        strategy_name=scroll_container.get_attribute("class") or self.config.scroll_strategy,
                        visible_cards=visible_count,
                        collected=len(records),
                        new_in_round=len(records) - before_count,
                        metrics=metrics,
                        last_review_id=last_review_id_before_collect,
                        new_cards_detected=False,
                        stop_reason=f"reached_max_reviews:{self.config.max_reviews}",
                        container_signature=container_signature,
                        container_review_count=len(cards_before_collect),
                        known_review_id_count_before=len(known_review_ids_before),
                        known_review_id_count_after=len(known_review_ids_before),
                        new_review_ids_detected=[],
                        spinner_seen=False,
                        mutation_seen=False,
                        batch_loaded_reason="reached_max_reviews",
                    )
                    self._debug_print(f"stop_reason: reached_max_reviews={self.config.max_reviews}")
                    return records

            new_in_round = len(records) - before_count
            metrics = self._get_scroll_metrics(driver, scroll_container)
            last_round_metrics = metrics
            current_strategy_name = (
                self.config.scroll_strategy
                if self.config.scroll_strategy != "baseline"
                else ["pane_step_scroll", "last_card_into_view", "action_chain_to_last_card"][
                    min(int(self.debug_info.get("_lazy_strategy_index", 0)), 2)
                ]
            )
            self._debug_print(
                f"round={round_index} strategy={current_strategy_name} visible_cards={visible_count} collected={len(records)} new_in_round={new_in_round} last_review_id={last_review_id_before_collect or 'none'} scroll_top={metrics['scroll_top']} scroll_height={metrics['scroll_height']} client_height={metrics['client_height']}"
            )

            if new_in_round == 0:
                idle_rounds += 1
                if len(records) > 0:
                    consecutive_existing_rounds += 1
            else:
                idle_rounds = 0
                consecutive_existing_rounds = 0

            observer_key = self._prepare_container_observer(driver, scroll_container)
            scroll_result = self._scroll_reviews_panel(
                driver,
                scroll_container,
                last_card=cards_before_collect[-1] if cards_before_collect else None,
            )
            batch_result = self._wait_for_review_batch(
                driver=driver,
                reviews_panel=scroll_container,
                previous_visible_count=visible_count,
                previous_scroll_height=metrics.get("scroll_height", -1),
                previous_last_card_signature=last_card_signature_before_collect,
                known_review_ids_before=known_review_ids_before,
                observer_key=observer_key,
            )

            if batch_result["loaded"]:
                self.debug_info["_lazy_strategy_index"] = 0
            else:
                self.debug_info["_lazy_strategy_index"] = min(int(self.debug_info.get("_lazy_strategy_index", 0)) + 1, 2)

            current_scroll_top = int(scroll_result.get("scroll_top", -1))
            if current_scroll_top <= previous_scroll_top:
                stagnant_scroll_rounds += 1
            else:
                stagnant_scroll_rounds = 0
            previous_scroll_top = current_scroll_top

            round_stop_reason = None
            if batch_result["visible_count"] < max(1, visible_count // 2):
                round_stop_reason = "unstable_visible_cards"
                stop_reason = round_stop_reason
                self._debug_print("stop_reason: unstable_visible_cards")
            elif (
                scroll_result.get("container_scroll_write_failed")
                and idle_rounds >= 2
                and not batch_result["new_review_ids_detected"]
                and batch_result["visible_count"] <= visible_count
                and batch_result.get("scroll_height", -1) <= metrics.get("scroll_height", -1)
            ):
                round_stop_reason = "container_scroll_write_failed"
                stop_reason = round_stop_reason
                self._debug_print("stop_reason: container_scroll_write_failed")
            elif (
                scroll_result.get("map_view_interference_detected")
                or (
                    scroll_result.get("outer_page_scroll_detected")
                    and not scroll_result.get("panel_scroll_response_detected")
                )
            ):
                round_stop_reason = "panel_interaction_failed"
                stop_reason = round_stop_reason
                self._debug_print("stop_reason: panel_interaction_failed")
            elif (
                existing_identities
                and consecutive_existing_rounds >= existing_stop_threshold
                and len(records) > 0
                and self._is_near_scroll_bottom(batch_result.get("metrics", metrics))
            ):
                round_stop_reason = "reached_end_of_reviews"
                stop_reason = round_stop_reason
                self._debug_print("stop_reason: reached_end_of_reviews")
            elif idle_rounds >= self.config.max_idle_rounds and self._is_near_scroll_bottom(batch_result.get("metrics", metrics)):
                round_stop_reason = "reached_end_of_reviews"
                stop_reason = round_stop_reason
                self._debug_print("stop_reason: reached_end_of_reviews")
            elif stagnant_scroll_rounds >= 3 and not batch_result["loaded"]:
                round_stop_reason = "lazy_load_not_triggered"
                stop_reason = round_stop_reason
                self._debug_print(f"stop_reason: {round_stop_reason}")

            self._record_round_log(
                round_index=round_index,
                strategy_name=scroll_result.get("lazy_load_strategy", current_strategy_name),
                visible_cards=visible_count,
                collected=len(records),
                new_in_round=new_in_round,
                metrics=metrics,
                last_review_id=last_review_id_before_collect,
                new_cards_detected=batch_result["loaded"],
                stop_reason=round_stop_reason,
                wait_result=batch_result,
                container_signature=container_signature,
                container_review_count=len(cards_before_collect),
                known_review_id_count_before=len(known_review_ids_before),
                known_review_id_count_after=len(batch_result["known_review_ids_after"]),
                new_review_ids_detected=batch_result["new_review_ids_detected"],
                spinner_seen=batch_result["spinner_seen"],
                mutation_seen=batch_result["mutation_seen"],
                batch_loaded_reason=batch_result["reason"],
                panel_focus_succeeded=scroll_result.get("panel_focus_succeeded", False),
                interaction_verified=scroll_result.get("interaction_verified", False),
                panel_scroll_response_detected=scroll_result.get("panel_scroll_response_detected", False),
                outer_page_scroll_detected=scroll_result.get("outer_page_scroll_detected", False),
                map_view_interference_detected=scroll_result.get("map_view_interference_detected", False),
                panel_interaction_mode=scroll_result.get("interaction_mode", self.config.panel_interaction_mode),
                extra_fields={
                    "lazy_load_strategy": scroll_result.get("lazy_load_strategy", current_strategy_name),
                    "strategy_attempt_index": scroll_result.get("strategy_attempt_index", -1),
                    "before_scroll_top": scroll_result.get("before_scroll_top", -1),
                    "after_scroll_top": scroll_result.get("after_scroll_top", -1),
                    "before_scroll_height": scroll_result.get("before_scroll_height", -1),
                    "after_scroll_height": scroll_result.get("after_scroll_height", -1),
                    "before_review_id_count": scroll_result.get("before_review_id_count", 0),
                    "after_review_id_count": scroll_result.get("after_review_id_count", 0),
                    "scroll_write_succeeded": scroll_result.get("scroll_write_succeeded", False),
                    "scroll_delta": scroll_result.get("scroll_delta", 0),
                    "container_scroll_write_failed": scroll_result.get("container_scroll_write_failed", False),
                    "step_count_in_round": scroll_result.get("step_count_in_round", 0),
                    "step_targets": scroll_result.get("step_targets", []),
                    "step_scroll_tops": scroll_result.get("step_scroll_tops", []),
                    "distance_to_bottom_before": scroll_result.get("distance_to_bottom_before", -1),
                    "distance_to_bottom_after": scroll_result.get("distance_to_bottom_after", -1),
                    "near_bottom_before": scroll_result.get("near_bottom_before", False),
                    "near_bottom_after": scroll_result.get("near_bottom_after", False),
                    "before_last_review_id": last_review_id_before_collect,
                    "after_last_review_id": batch_result["known_review_ids_after"][-1] if batch_result["known_review_ids_after"] else None,
                    "review_count_delta": len(batch_result["known_review_ids_after"]) - len(known_review_ids_before),
                    "scroll_height_delta": batch_result.get("scroll_height", -1) - metrics.get("scroll_height", -1),
                    "wait_result_reason": batch_result.get("reason"),
                    "wait_duration_seconds": batch_result.get("wait_duration_seconds", 0.0),
                },
            )
            if round_stop_reason:
                break
            self._sleep_scroll()

        self.debug_info["final_stop_reason"] = stop_reason
        self.debug_info["final_scroll_metrics"] = last_round_metrics
        self.debug_info["strategy_verdict"] = self._determine_strategy_verdict()
        if records:
            self.debug_info["scrape_verdict"] = "captured_real_reviews"
        elif stop_reason in {"lazy_load_not_triggered", "reached_end_of_reviews"}:
            self.debug_info["scrape_verdict"] = stop_reason
        else:
            self.debug_info["scrape_verdict"] = "no_reviews_captured"
        return records

    def _resolve_scroll_container(self, driver: webdriver.Chrome, fallback_panel: WebElement) -> WebElement:
        verified_selector = "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde"
        try:
            verified_elements = driver.find_elements(By.CSS_SELECTOR, verified_selector)
        except WebDriverException:
            verified_elements = []
        for element in verified_elements:
            try:
                summary = self._build_container_candidate_summary(driver, element)
            except (StaleElementReferenceException, WebDriverException):
                continue
            if (
                summary.get("overflow_y") in {"scroll", "auto"}
                and summary.get("scroll_height", 0) > summary.get("client_height", 0) + 40
                and summary.get("review_count", 0) > 0
            ):
                summary["selection_mode"] = "verified_selector"
                self.debug_info["scroll_container_candidates"] = [summary]
                self._debug_print(
                    f"selected_scroll_container: review_count={summary['review_count']} scroll_height={summary['scroll_height']} class={summary['class_name']}"
                )
                return element

        candidates: list[tuple[int, WebElement, dict[str, Any]]] = []
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, "div.m6QErb, div.DxyBCb")
        except WebDriverException:
            elements = []

        for element in elements:
            try:
                style = driver.execute_script(
                    "const s = window.getComputedStyle(arguments[0]); return {overflowY: s.overflowY, scrollHeight: Math.floor(arguments[0].scrollHeight || 0), clientHeight: Math.floor(arguments[0].clientHeight || 0)};",
                    element,
                )
                class_name = element.get_attribute("class") or ""
                role = element.get_attribute("role") or ""
                review_count = len(self._find_review_cards(element))
            except (StaleElementReferenceException, WebDriverException):
                continue
            overflow_y = style.get("overflowY", "")
            scroll_height = int(style.get("scrollHeight", 0))
            client_height = int(style.get("clientHeight", 0))
            if not (("m6QErb" in class_name or "DxyBCb" in class_name) and overflow_y in {"scroll", "auto"} and scroll_height > client_height + 40 and review_count > 0):
                continue
            score = review_count * 100000 + scroll_height
            if "m6QErb" in class_name:
                score += 5000
            if "DxyBCb" in class_name:
                score += 3000
            if "kA9KIf" in class_name:
                score += 1000
            summary = {
                "class_name": class_name,
                "role": role,
                "aria_label": element.get_attribute("aria-label") or "",
                "overflow_y": overflow_y,
                "scroll_height": scroll_height,
                "client_height": client_height,
                "scroll_top": self._get_scroll_metrics(driver, element).get("scroll_top", -1),
                "review_count": review_count,
                "score": score,
                "selection_mode": "fallback_candidates",
            }
            candidates.append((score, element, summary))

        candidates.sort(key=lambda item: item[0], reverse=True)
        self.debug_info["scroll_container_candidates"] = [item[2] for item in candidates]
        if candidates:
            selected = candidates[0]
            self._debug_print(
                f"selected_scroll_container: review_count={selected[2]['review_count']} scroll_height={selected[2]['scroll_height']} class={selected[2]['class_name']}"
            )
            return selected[1]
        return self._resolve_scroll_container_from_cards(driver, fallback_panel)

    def _resolve_scroll_container_from_cards(self, driver: webdriver.Chrome, fallback_panel: WebElement) -> WebElement:
        cards = self._find_review_cards(driver)
        if not cards:
            return fallback_panel
        try:
            container = driver.execute_script(
                """
                const cards = arguments[0];
                let bestNode = null;
                let bestScore = -1;
                for (const card of cards.slice(0, 8)) {
                  let node = card;
                  let depth = 0;
                  while (node && depth < 10) {
                    const style = window.getComputedStyle(node);
                    const overflowY = style.overflowY;
                    const scrollHeight = Math.floor(node.scrollHeight || 0);
                    const clientHeight = Math.floor(node.clientHeight || 0);
                    const isScrollable = (overflowY === 'auto' || overflowY === 'scroll') && scrollHeight > clientHeight + 40;
                    if (isScrollable) {
                      const score = scrollHeight;
                      if (score > bestScore) {
                        bestNode = node;
                        bestScore = score;
                      }
                    }
                    node = node.parentElement;
                    depth += 1;
                  }
                }
                return bestNode;
                """,
                cards,
            )
            return container or fallback_panel
        except WebDriverException:
            return fallback_panel

    def _is_near_scroll_bottom(self, metrics: dict[str, int]) -> bool:
        scroll_top = metrics.get("scroll_top", -1)
        scroll_height = metrics.get("scroll_height", -1)
        client_height = metrics.get("client_height", -1)
        if min(scroll_top, scroll_height, client_height) < 0:
            return False
        return scroll_top + client_height >= scroll_height - 120

    def _distance_to_scroll_bottom(self, metrics: dict[str, int]) -> int:
        scroll_top = int(metrics.get("scroll_top", -1))
        scroll_height = int(metrics.get("scroll_height", -1))
        client_height = int(metrics.get("client_height", -1))
        if min(scroll_top, scroll_height, client_height) < 0:
            return -1
        return max(0, scroll_height - client_height - scroll_top)

    def _wait_for_review_batch(
        self,
        *,
        driver: webdriver.Chrome,
        reviews_panel: WebElement,
        previous_visible_count: int,
        previous_scroll_height: int,
        previous_last_card_signature: str,
        known_review_ids_before: set[str],
        observer_key: str,
    ) -> dict[str, Any]:
        min_wait_seconds = 2.0
        poll_seconds = 0.5
        start_time = time.time()
        deadline = start_time + 8.0
        last_visible_count = previous_visible_count
        last_signature = previous_last_card_signature
        stable_rounds = 0
        spinner_seen = False
        result: dict[str, Any] = {
            "loaded": False,
            "visible_count": previous_visible_count,
            "scroll_height": previous_scroll_height,
            "last_card_changed": False,
            "reason": "timeout",
            "known_review_ids_after": sorted(known_review_ids_before),
            "new_review_ids_detected": [],
            "spinner_seen": False,
            "mutation_seen": False,
            "metrics": {"scroll_top": -1, "scroll_height": previous_scroll_height, "client_height": -1},
            "wait_duration_seconds": 0.0,
        }
        while time.time() < deadline:
            time.sleep(poll_seconds)
            current_cards = self._find_review_cards(reviews_panel)
            visible_count = len(current_cards)
            metrics = self._get_scroll_metrics(driver, reviews_panel)
            current_signature = self._review_card_signature(current_cards[-1]) if current_cards else ""
            current_review_ids = self._review_ids_in_container(reviews_panel)
            new_review_ids = sorted(current_review_ids - known_review_ids_before)
            loading_visible = self._loading_indicator_visible(reviews_panel)
            spinner_seen = spinner_seen or loading_visible
            mutation_seen = self._read_container_observer(driver, reviews_panel, observer_key)
            loaded_reason = None
            if new_review_ids:
                loaded_reason = "new_review_ids"
            elif visible_count > previous_visible_count:
                loaded_reason = "visible_cards_increase"
            elif metrics.get("scroll_height", -1) > previous_scroll_height:
                loaded_reason = "scroll_height_increase"
            elif current_signature and current_signature != previous_last_card_signature:
                loaded_reason = "last_review_id_changed"
            result.update(
                {
                    "visible_count": visible_count,
                    "scroll_height": metrics.get("scroll_height", -1),
                    "last_card_changed": bool(current_signature and current_signature != previous_last_card_signature),
                    "known_review_ids_after": sorted(current_review_ids),
                    "new_review_ids_detected": new_review_ids[:10],
                    "spinner_seen": spinner_seen,
                    "mutation_seen": mutation_seen,
                    "metrics": metrics,
                    "wait_duration_seconds": round(time.time() - start_time, 2),
                }
            )
            if loaded_reason:
                result["loaded"] = True
                result["reason"] = loaded_reason
                self._debug_print(
                    f"batch_loaded: reason={loaded_reason} visible_cards={visible_count} new_review_ids={len(new_review_ids)} scroll_height={metrics.get('scroll_height', -1)}"
                )
                return result
            if visible_count == last_visible_count and current_signature == last_signature:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_visible_count = visible_count
            last_signature = current_signature
            if time.time() - start_time >= min_wait_seconds and stable_rounds >= 10:
                result["reason"] = "stable_timeout"
                break
        self._debug_print(
            f"batch_wait_timeout: visible_cards={result['visible_count']} spinner_seen={result['spinner_seen']} mutation_seen={result['mutation_seen']}"
        )
        return result

    def _record_round_log(
        self,
        *,
        round_index: int,
        strategy_name: str,
        visible_cards: int,
        collected: int,
        new_in_round: int,
        metrics: dict[str, int],
        last_review_id: str | None,
        new_cards_detected: bool,
        stop_reason: str | None,
        container_signature: str,
        container_review_count: int,
        known_review_id_count_before: int,
        known_review_id_count_after: int,
        new_review_ids_detected: list[str],
        spinner_seen: bool,
        mutation_seen: bool,
        batch_loaded_reason: str | None,
        wait_result: dict[str, Any] | None = None,
        panel_focus_succeeded: bool = False,
        interaction_verified: bool = False,
        panel_scroll_response_detected: bool = False,
        outer_page_scroll_detected: bool = False,
        map_view_interference_detected: bool = False,
        panel_interaction_mode: str | None = None,
        extra_fields: dict[str, Any] | None = None,
    ) -> None:
        log_item = {
            "round": round_index,
            "strategy_name": strategy_name,
            "visible_cards": visible_cards,
            "collected": collected,
            "new_in_round": new_in_round,
            "new_cards_detected": new_cards_detected,
            "last_review_id": last_review_id,
            "scroll_metrics": metrics,
            "stop_reason": stop_reason,
            "wait_result": wait_result,
            "container_signature": container_signature,
            "container_review_count": container_review_count,
            "known_review_id_count_before": known_review_id_count_before,
            "known_review_id_count_after": known_review_id_count_after,
            "new_review_ids_detected": new_review_ids_detected,
            "spinner_seen": spinner_seen,
            "mutation_seen": mutation_seen,
            "batch_loaded_reason": batch_loaded_reason,
            "panel_focus_succeeded": panel_focus_succeeded,
            "interaction_verified": interaction_verified,
            "panel_scroll_response_detected": panel_scroll_response_detected,
            "outer_page_scroll_detected": outer_page_scroll_detected,
            "map_view_interference_detected": map_view_interference_detected,
            "panel_interaction_mode": panel_interaction_mode,
        }
        if extra_fields:
            log_item.update(extra_fields)
        self.debug_info.setdefault("round_logs", []).append(log_item)

    def _container_signature(self, container: WebElement) -> str:
        try:
            class_name = container.get_attribute("class") or ""
            role = container.get_attribute("role") or ""
            aria = container.get_attribute("aria-label") or ""
            return f"class={class_name}||role={role}||aria={aria}"
        except StaleElementReferenceException:
            return "stale-container"

    def _build_container_candidate_summary(self, driver: webdriver.Chrome, container: WebElement) -> dict[str, Any]:
        try:
            metrics = self._get_scroll_metrics(driver, container)
            overflow_y = (
                driver.execute_script("return window.getComputedStyle(arguments[0]).overflowY;", container) or ""
            )
            return {
                "class_name": container.get_attribute("class") or "",
                "role": container.get_attribute("role") or "",
                "aria_label": container.get_attribute("aria-label") or "",
                "overflow_y": overflow_y,
                "scroll_height": metrics.get("scroll_height", -1),
                "client_height": metrics.get("client_height", -1),
                "scroll_top": metrics.get("scroll_top", -1),
                "review_count": len(self._find_review_cards(container)),
                "signature": self._container_signature(container),
            }
        except (StaleElementReferenceException, WebDriverException):
            return {"signature": "unavailable", "review_count": 0}

    def _determine_review_view_mode(self, container_summary: dict[str, Any]) -> str:
        class_name = container_summary.get("class_name", "")
        overflow_y = container_summary.get("overflow_y", "")
        scroll_height = int(container_summary.get("scroll_height", 0))
        client_height = int(container_summary.get("client_height", 0))
        review_count = int(container_summary.get("review_count", 0))
        if (
            "DxyBCb" in class_name
            and "dS8AEf" in class_name
            and overflow_y in {"auto", "scroll"}
            and scroll_height > client_height + 40
            and review_count > 0
        ):
            return "full_review_list"
        if review_count > 0:
            return "embedded_review_section"
        return "unknown"

    def _review_ids_in_container(self, container: WebElement) -> set[str]:
        review_ids: set[str] = set()
        for card in self._find_review_cards(container):
            review_id = self._extract_review_id(card)
            if review_id:
                review_ids.add(review_id)
        return review_ids

    def _prepare_container_observer(self, driver: webdriver.Chrome, container: WebElement) -> str:
        observer_key = f"codex_observer_{self.scrape_run_id}_{int(time.time() * 1000)}"
        try:
            driver.execute_script(
                """
                const key = arguments[1];
                const node = arguments[0];
                if (!window.__codexMutationFlags) { window.__codexMutationFlags = {}; }
                window.__codexMutationFlags[key] = false;
                const observer = new MutationObserver(() => { window.__codexMutationFlags[key] = true; });
                observer.observe(node, {childList: true, subtree: true});
                setTimeout(() => observer.disconnect(), 12000);
                """,
                container,
                observer_key,
            )
        except WebDriverException:
            pass
        return observer_key

    def _read_container_observer(self, driver: webdriver.Chrome, container: WebElement, observer_key: str) -> bool:
        try:
            return bool(
                driver.execute_script(
                    "return !!(window.__codexMutationFlags && window.__codexMutationFlags[arguments[0]]);",
                    observer_key,
                )
            )
        except WebDriverException:
            return False

    def _loading_indicator_visible(self, container: WebElement) -> bool:
        selectors = [
            "[role='progressbar']",
            "div[class*='loading']",
            "div[class*='spinner']",
            "div[jsaction*='pane.review']",
        ]
        for selector in selectors:
            try:
                if container.find_elements(By.CSS_SELECTOR, selector):
                    return True
            except (StaleElementReferenceException, NoSuchElementException):
                continue
        return False

    def _determine_strategy_verdict(self) -> str:
        round_logs = self.debug_info.get("round_logs", [])
        if any((item.get("new_review_ids_detected") or []) for item in round_logs):
            return "effective"
        if any(item.get("stop_reason") in {"unstable_visible_cards", "panel_interaction_failed"} for item in round_logs):
            return "unstable"
        if any(item.get("stop_reason") == "lazy_load_not_triggered" for item in round_logs):
            return "stalled"
        if self.debug_info.get("error"):
            return "unstable"
        return "partial"

    def _get_scroll_metrics(self, driver: webdriver.Chrome, container: WebElement) -> dict[str, int]:
        try:
            scroll_top, scroll_height, client_height = driver.execute_script(
                "return [Math.floor(arguments[0].scrollTop), Math.floor(arguments[0].scrollHeight), Math.floor(arguments[0].clientHeight)];",
                container,
            )
            return {
                "scroll_top": int(scroll_top),
                "scroll_height": int(scroll_height),
                "client_height": int(client_height),
            }
        except WebDriverException:
            return {"scroll_top": -1, "scroll_height": -1, "client_height": -1}

    def _get_window_scroll_y(self, driver: webdriver.Chrome) -> int:
        try:
            return int(driver.execute_script("return Math.floor(window.pageYOffset || document.documentElement.scrollTop || document.body.scrollTop || 0);") or 0)
        except WebDriverException:
            return -1

    def _probe_panel_interaction(self, driver: webdriver.Chrome, reviews_panel: WebElement) -> dict[str, Any]:
        before_metrics = self._get_scroll_metrics(driver, reviews_panel)
        before_window_scroll = self._get_window_scroll_y(driver)
        focus_succeeded = False
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", reviews_panel)
            time.sleep(random.uniform(0.15, 0.3))
            try:
                ActionChains(driver).move_to_element(reviews_panel).pause(random.uniform(0.08, 0.18)).click(reviews_panel).perform()
                focus_succeeded = True
            except WebDriverException:
                pass
            driver.execute_script("arguments[0].setAttribute('tabindex', '-1'); arguments[0].focus();", reviews_panel)
            focus_succeeded = True
            reviews_panel.send_keys(Keys.PAGE_DOWN)
            time.sleep(random.uniform(0.18, 0.32))
        except (WebDriverException, StaleElementReferenceException):
            focus_succeeded = False
        after_metrics = self._get_scroll_metrics(driver, reviews_panel)
        after_window_scroll = self._get_window_scroll_y(driver)
        panel_scroll_response_detected = after_metrics.get("scroll_top", -1) > before_metrics.get("scroll_top", -1)
        outer_page_scroll_detected = after_window_scroll > before_window_scroll >= 0
        map_view_interference_detected = outer_page_scroll_detected and not panel_scroll_response_detected
        try:
            if before_metrics.get("scroll_top", -1) >= 0:
                driver.execute_script("arguments[0].scrollTop = arguments[1];", reviews_panel, before_metrics.get("scroll_top", 0))
            if before_window_scroll >= 0:
                driver.execute_script("window.scrollTo(0, arguments[0]);", before_window_scroll)
        except WebDriverException:
            pass
        return {
            "panel_focus_succeeded": focus_succeeded,
            "interaction_verified": focus_succeeded and panel_scroll_response_detected and not map_view_interference_detected,
            "panel_scroll_response_detected": panel_scroll_response_detected,
            "outer_page_scroll_detected": outer_page_scroll_detected,
            "map_view_interference_detected": map_view_interference_detected,
            "interaction_mode": "probe_pagedown",
        }

    def _build_scroll_result(
        self,
        *,
        interaction_mode: str,
        panel_focus_succeeded: bool,
        interaction_verified: bool,
        panel_scroll_response_detected: bool,
        outer_page_scroll_detected: bool,
        map_view_interference_detected: bool,
        scroll_top: int,
    ) -> dict[str, Any]:
        return {
            "interaction_mode": interaction_mode,
            "panel_focus_succeeded": panel_focus_succeeded,
            "interaction_verified": interaction_verified,
            "panel_scroll_response_detected": panel_scroll_response_detected,
            "outer_page_scroll_detected": outer_page_scroll_detected,
            "map_view_interference_detected": map_view_interference_detected,
            "scroll_top": int(scroll_top),
        }

    def _focus_reviews_panel(self, driver: webdriver.Chrome, reviews_panel: WebElement) -> bool:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", reviews_panel)
            time.sleep(random.uniform(0.15, 0.3))
            try:
                ActionChains(driver).move_to_element(reviews_panel).pause(random.uniform(0.08, 0.18)).click(reviews_panel).perform()
            except WebDriverException:
                pass
            driver.execute_script("arguments[0].setAttribute('tabindex', '-1'); arguments[0].focus();", reviews_panel)
            return True
        except (WebDriverException, StaleElementReferenceException):
            return False

    def _expand_visible_review_bodies(self, driver: webdriver.Chrome, reviews_panel: WebElement) -> None:
        clicked = 0
        original_scroll_top = self._get_scroll_metrics(driver, reviews_panel).get("scroll_top", -1)
        cards = self._find_review_cards(reviews_panel) or self._find_review_cards(driver)
        for card in cards[:30]:
            clicked += self._expand_review_card(driver, card)
        if original_scroll_top >= 0:
            try:
                driver.execute_script("arguments[0].scrollTop = arguments[1];", reviews_panel, original_scroll_top)
            except WebDriverException:
                pass
        if clicked:
            self._record_event(f"expanded_more_buttons:{clicked}")

    def _expand_review_card(self, driver: webdriver.Chrome, card: WebElement) -> int:
        selectors = [
            "button.w8nwRe",
            "button[jsaction*='expandReview']",
            "button[aria-label*='\u986f\u793a\u66f4\u591a']",
            "button[aria-label*='\u66f4\u591a']",
            "button[aria-label*='Show more']",
        ]
        buttons: list[WebElement] = []
        for selector in selectors:
            try:
                buttons.extend(card.find_elements(By.CSS_SELECTOR, selector))
            except (NoSuchElementException, StaleElementReferenceException):
                continue

        clicked = 0
        seen: set[str] = set()
        for button in buttons:
            try:
                aria_label = button.get_attribute("aria-label") or ""
                text = button.text or ""
                signature = f"{aria_label}||{text}||{button.get_attribute('data-review-id') or ''}"
                if signature in seen:
                    continue
                seen.add(signature)
                if button.get_attribute("aria-expanded") == "true":
                    continue
                label_text = f"{aria_label} {text}".lower()
                if not any(token in label_text for token in ["\u986f\u793a\u66f4\u591a", "\u66f4\u591a", "show more", "more"]):
                    continue
                driver.execute_script("arguments[0].click();", button)
                clicked += 1
                time.sleep(random.uniform(0.2, 0.45))
            except (WebDriverException, StaleElementReferenceException):
                continue
        return clicked

    def _find_review_cards(self, root: Any) -> list[WebElement]:
        driver = root if isinstance(root, webdriver.Chrome) else None
        container = None if isinstance(root, webdriver.Chrome) else root
        if driver is None:
            try:
                driver = root.parent
                container = root
            except Exception:
                driver = None
                container = None

        if driver is not None and container is not None:
            try:
                cards = driver.execute_script(
                    """
                    const container = arguments[0];
                    const all = Array.from(container.querySelectorAll('[data-review-id]'));
                    return all.filter(el => {
                      const ancestor = el.parentElement ? el.parentElement.closest('[data-review-id]') : null;
                      return !ancestor;
                    });
                    """,
                    container,
                )
                if cards:
                    seen_ids: set[str] = set()
                    result: list[WebElement] = []
                    for card in cards:
                        try:
                            review_id = card.get_attribute("data-review-id") or ""
                        except StaleElementReferenceException:
                            continue
                        if not review_id or review_id in seen_ids:
                            continue
                        seen_ids.add(review_id)
                        result.append(card)
                    if result:
                        return result
            except WebDriverException:
                pass

        search_root = container if container is not None else root
        try:
            cards = search_root.find_elements(By.CSS_SELECTOR, '[data-review-id]')
        except (AttributeError, StaleElementReferenceException, WebDriverException):
            return []
        seen_ids: set[str] = set()
        result: list[WebElement] = []
        for card in cards:
            try:
                review_id = card.get_attribute("data-review-id") or ""
                has_review_ancestor = bool(card.find_elements(By.XPATH, './ancestor::*[@data-review-id][1]'))
            except StaleElementReferenceException:
                continue
            if not review_id or review_id in seen_ids or has_review_ancestor:
                continue
            seen_ids.add(review_id)
            result.append(card)
        return result

    def _review_card_signature(self, card: WebElement) -> str:
        try:
            review_id = card.get_attribute("data-review-id") or ""
            if review_id:
                return f"review-id::{review_id}"
            ancestor_id = card.find_element(By.XPATH, "ancestor-or-self::*[@data-review-id][1]").get_attribute("data-review-id") or ""
            if ancestor_id:
                return f"ancestor-review-id::{ancestor_id}"
        except Exception:
            pass
        try:
            text = self._normalize_whitespace(card.text) or ""
        except StaleElementReferenceException:
            text = ""
        return f"text::{text[:120]}"

    def _looks_like_review_card(self, card: WebElement) -> bool:
        try:
            review_id = card.get_attribute("data-review-id") or ""
            class_name = card.get_attribute("class") or ""
            text = card.text.strip()
            html = (card.get_attribute("innerHTML") or "")[:800]
        except StaleElementReferenceException:
            return False
        if review_id:
            return True
        if "jftiEf" in class_name:
            return True
        return bool(re.search(r"rsqaWe|d4r55|wiI7pd|stars?", html, flags=re.IGNORECASE) or re.search(r"days ago|weeks ago|months ago|years ago", text, flags=re.IGNORECASE))

    def _extract_review_id(self, card: WebElement) -> str | None:
        try:
            review_id = card.get_attribute("data-review-id")
            if review_id:
                return review_id
        except StaleElementReferenceException:
            return None
        try:
            return card.find_element(By.XPATH, "ancestor-or-self::*[@data-review-id][1]").get_attribute("data-review-id") or None
        except Exception:
            return None

    def _resolve_review_card(self, card: WebElement) -> WebElement:
        try:
            resolved = card.find_element(By.XPATH, "ancestor-or-self::div[contains(@class, 'jftiEf')][1]")
            if resolved is not None:
                return resolved
        except Exception:
            pass
        try:
            resolved = card.find_element(By.XPATH, "ancestor-or-self::*[@data-review-id][1]")
            if resolved is not None:
                return resolved
        except Exception:
            pass
        return card

    def _extract_review_from_card(
        self,
        *,
        card: WebElement,
        place_id: str,
        place_name: str | None,
        source_url: str,
    ) -> ReviewRecord | None:
        review_card = self._resolve_review_card(card)
        reviewer_name = self._safe_text(review_card, [".d4r55", ".TSUbDb", "div.d4r55"])
        review_text = self._extract_review_text(review_card)
        star_rating = self._extract_star_rating(review_card)
        review_date_text = self._safe_text(review_card, [".rsqaWe", "span.rsqaWe"])
        owner_response_text = self._safe_text(review_card, [".CDe7pd", ".wiI7pd[aria-label*='??????']"])
        owner_response_date = self._extract_owner_response_date(review_card)
        reviewer_profile_url = self._safe_attribute(review_card, "a[href*='/contrib/']", "href")
        likes_count = self._extract_likes_count(review_card)
        raw_review_id = self._extract_review_id(review_card)
        raw_review_metadata = {
            "review_id": raw_review_id,
            "relative_date_text": review_date_text,
            "google_place_token": self.debug_info.get("google_place_token"),
        }
        scraped_at = datetime.now(UTC).isoformat()
        review_date_estimated = self._estimate_review_date(review_date_text)
        review_unique_key = self._build_review_unique_key(
            place_id=place_id,
            raw_review_id=raw_review_id,
            reviewer_name=reviewer_name,
            star_rating=star_rating,
            review_date_text=review_date_text,
            review_text=review_text,
        )

        if not reviewer_name and not review_text:
            return None

        return ReviewRecord(
            place_id=place_id,
            place_name=place_name,
            source_url=source_url,
            reviewer_name=reviewer_name,
            review_text=review_text,
            star_rating=star_rating,
            review_date_text=review_date_text,
            owner_response_text=owner_response_text,
            owner_response_date=owner_response_date,
            scraped_at=scraped_at,
            review_unique_key=review_unique_key,
            review_date_estimated=review_date_estimated,
            scrape_run_id=self.scrape_run_id,
            review_language=None,
            reviewer_profile_url=reviewer_profile_url,
            likes_count=likes_count,
            raw_review_metadata=raw_review_metadata,
        )

    def _extract_review_text(self, card: WebElement) -> str | None:
        candidates: list[str] = []

        try:
            expanded_text = card.parent.execute_script(
                """
                const card = arguments[0];
                const containers = Array.from(card.querySelectorAll('.MyEned, .review-full-text, [data-review-text], [id^="ucc-"]'));
                const texts = [];
                for (const container of containers) {
                  const clone = container.cloneNode(true);
                  clone.querySelectorAll('button, [role="button"]').forEach((node) => node.remove());
                  const text = (clone.innerText || clone.textContent || '').trim();
                  if (text) {
                    texts.push(text);
                  }
                }
                return texts;
                """,
                card,
            )
            for item in expanded_text or []:
                text = self._normalize_whitespace(item)
                if text:
                    candidates.append(text)
        except WebDriverException:
            pass

        selectors = [
            ".MyEned .wiI7pd",
            ".wiI7pd",
            "div.MyEned",
            ".review-full-text",
        ]
        for selector in selectors:
            try:
                elements = card.find_elements(By.CSS_SELECTOR, selector)
            except (NoSuchElementException, StaleElementReferenceException):
                continue
            for element in elements:
                text = self._normalize_whitespace(element.text)
                if text:
                    candidates.append(text)

        cleaned_candidates = []
        for candidate in candidates:
            lowered = candidate.lower()
            cleaned = candidate
            for suffix in ["????", "??", "show more"]:
                if lowered.endswith(suffix.lower()):
                    cleaned = candidate[: -len(suffix)].rstrip()
                    break
            cleaned = cleaned.strip()
            if cleaned:
                cleaned_candidates.append(cleaned)
        if not cleaned_candidates:
            return None
        cleaned_candidates = sorted(set(cleaned_candidates), key=len, reverse=True)
        return cleaned_candidates[0]

    def _extract_star_rating(self, card: WebElement) -> float | None:
        candidates = [
            self._safe_attribute(card, "span[role='img']", "aria-label"),
            self._safe_attribute(card, "span.kvMYJc", "aria-label"),
        ]
        for text in candidates:
            if not text:
                continue
            match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
            if match:
                return float(match.group(1))
        return None

    def _extract_owner_response_date(self, card: WebElement) -> str | None:
        texts = [item.text.strip() for item in card.find_elements(By.CSS_SELECTOR, "span") if item.text.strip()]
        for text in texts:
            if "店家回覆" in text or "Response from the owner" in text:
                return text
        return None

    def _extract_likes_count(self, card: WebElement) -> int | None:
        texts = [item.text.strip() for item in card.find_elements(By.CSS_SELECTOR, "button, span") if item.text.strip()]
        for text in texts:
            if text.isdigit():
                return int(text)
            match = re.search(r"([0-9]+)\s*(?:人按讚|likes?)", text)
            if match:
                return int(match.group(1))
        return None

    def _scroll_reviews_panel(self, driver: webdriver.Chrome, reviews_panel: WebElement, last_card: WebElement | None = None) -> dict[str, Any]:
        strategy = self.config.scroll_strategy
        if strategy == "scroll_into_view":
            result = self._scroll_with_scroll_into_view(driver, reviews_panel, last_card)
            result["lazy_load_strategy"] = "last_card_into_view"
            result["strategy_attempt_index"] = 1
            return result
        if strategy == "action_chains":
            result = self._scroll_with_action_chains(driver, reviews_panel, last_card)
            result["lazy_load_strategy"] = "action_chain_to_last_card"
            result["strategy_attempt_index"] = 2
            return result
        if strategy == "hybrid":
            result = self._scroll_with_hybrid(driver, reviews_panel, last_card)
            result["lazy_load_strategy"] = "hybrid"
            result["strategy_attempt_index"] = 0
            return result
        return self._scroll_with_strategy_cycle(driver, reviews_panel, last_card)

    def _scroll_with_strategy_cycle(
        self,
        driver: webdriver.Chrome,
        reviews_panel: WebElement,
        last_card: WebElement | None,
    ) -> dict[str, Any]:
        strategy_cycle = [
            ("pane_step_scroll", self._scroll_with_baseline),
            ("last_card_into_view", lambda d, p: self._scroll_with_scroll_into_view(d, p, last_card)),
            ("action_chain_to_last_card", lambda d, p: self._scroll_with_action_chains(d, p, last_card)),
        ]
        strategy_attempt_index = int(self.debug_info.get("_lazy_strategy_index", 0))
        strategy_attempt_index = max(0, min(strategy_attempt_index, len(strategy_cycle) - 1))
        strategy_name, strategy_handler = strategy_cycle[strategy_attempt_index]
        result = strategy_handler(driver, reviews_panel)
        result["lazy_load_strategy"] = strategy_name
        result["strategy_attempt_index"] = strategy_attempt_index
        return result

    def _scroll_with_baseline(self, driver: webdriver.Chrome, reviews_panel: WebElement) -> dict[str, Any]:
        before_metrics = self._get_scroll_metrics(driver, reviews_panel)
        before_window_scroll = self._get_window_scroll_y(driver)
        panel_focus_succeeded = True
        if self.config.panel_focus_before_scroll:
            panel_focus_succeeded = self._focus_reviews_panel(driver, reviews_panel)
        interaction_mode = "js_scroll"
        before_review_ids = self._review_ids_in_container(reviews_panel)
        scroll_write_succeeded = False
        container_scroll_write_failed = False
        step_scroll_tops: list[int] = [before_metrics.get("scroll_top", -1)]
        step_targets: list[int] = []
        try:
            client_height = max(1, before_metrics.get("client_height", 0))
            scroll_height = max(0, before_metrics.get("scroll_height", 0))
            current_top = max(0, before_metrics.get("scroll_top", 0))
            step = max(120, int(client_height * 0.4))
            max_target = max(current_top, scroll_height - max(40, int(client_height * 0.2)))
            step_count = 3 if self._distance_to_scroll_bottom(before_metrics) > client_height else 2
            for _ in range(step_count):
                target_top = min(current_top + step, max_target)
                step_targets.append(target_top)
                driver.execute_script(
                    """
                    arguments[0].scrollTop = arguments[1];
                    arguments[0].dispatchEvent(new Event('scroll', {bubbles: true}));
                    arguments[0].dispatchEvent(new WheelEvent('wheel', {deltaY: arguments[2], bubbles: true, cancelable: true}));
                    """,
                    reviews_panel,
                    target_top,
                    step,
                )
                time.sleep(random.uniform(0.4, 0.8))
                current_top = int(driver.execute_script("return Math.floor(arguments[0].scrollTop);", reviews_panel) or current_top)
                step_scroll_tops.append(current_top)
            current_top_after = int(driver.execute_script("return Math.floor(arguments[0].scrollTop);", reviews_panel) or 0)
            scroll_write_succeeded = current_top_after > before_metrics.get("scroll_top", 0)
            container_scroll_write_failed = not scroll_write_succeeded
        except (WebDriverException, StaleElementReferenceException):
            panel_focus_succeeded = False
            container_scroll_write_failed = True
        after_metrics = self._get_scroll_metrics(driver, reviews_panel)
        after_window_scroll = self._get_window_scroll_y(driver)
        after_review_ids = self._review_ids_in_container(reviews_panel)
        panel_scroll_response_detected = after_metrics.get("scroll_top", -1) > before_metrics.get("scroll_top", -1)
        outer_page_scroll_detected = after_window_scroll > before_window_scroll >= 0
        map_view_interference_detected = outer_page_scroll_detected and not panel_scroll_response_detected
        result = self._build_scroll_result(
            interaction_mode=interaction_mode,
            panel_focus_succeeded=panel_focus_succeeded,
            interaction_verified=panel_scroll_response_detected and not map_view_interference_detected,
            panel_scroll_response_detected=panel_scroll_response_detected,
            outer_page_scroll_detected=outer_page_scroll_detected,
            map_view_interference_detected=map_view_interference_detected,
            scroll_top=after_metrics.get("scroll_top", -1),
        )
        result.update(
            {
                "before_scroll_top": before_metrics.get("scroll_top", -1),
                "after_scroll_top": after_metrics.get("scroll_top", -1),
                "before_scroll_height": before_metrics.get("scroll_height", -1),
                "after_scroll_height": after_metrics.get("scroll_height", -1),
                "before_review_id_count": len(before_review_ids),
                "after_review_id_count": len(after_review_ids),
                "scroll_write_succeeded": scroll_write_succeeded,
                "scroll_delta": max(0, after_metrics.get("scroll_top", -1) - before_metrics.get("scroll_top", -1)),
                "container_scroll_write_failed": container_scroll_write_failed,
                "step_count_in_round": max(0, len(step_scroll_tops) - 1),
                "step_targets": step_targets,
                "step_scroll_tops": step_scroll_tops,
                "distance_to_bottom_before": self._distance_to_scroll_bottom(before_metrics),
                "distance_to_bottom_after": self._distance_to_scroll_bottom(after_metrics),
                "near_bottom_before": self._is_near_scroll_bottom(before_metrics),
                "near_bottom_after": self._is_near_scroll_bottom(after_metrics),
            }
        )
        return result

    def _scroll_with_scroll_into_view(self, driver: webdriver.Chrome, reviews_panel: WebElement, last_card: WebElement | None) -> dict[str, Any]:
        panel_focus_succeeded = self._focus_reviews_panel(driver, reviews_panel)
        before_metrics = self._get_scroll_metrics(driver, reviews_panel)
        before_window_scroll = self._get_window_scroll_y(driver)
        before_review_ids = self._review_ids_in_container(reviews_panel)
        try:
            if last_card is not None:
                driver.execute_script("arguments[0].scrollIntoView({block: 'end', inline: 'nearest'});", last_card)
                time.sleep(random.uniform(0.4, 0.7))
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollTop + arguments[1]; arguments[0].dispatchEvent(new Event('scroll'));",
                reviews_panel,
                random.randint(120, 240),
            )
            time.sleep(random.uniform(0.25, 0.45))
        except WebDriverException:
            panel_focus_succeeded = False
        after_metrics = self._get_scroll_metrics(driver, reviews_panel)
        after_window_scroll = self._get_window_scroll_y(driver)
        after_review_ids = self._review_ids_in_container(reviews_panel)
        panel_scroll_response_detected = after_metrics.get("scroll_top", -1) > before_metrics.get("scroll_top", -1)
        outer_page_scroll_detected = after_window_scroll > before_window_scroll >= 0
        map_view_interference_detected = outer_page_scroll_detected and not panel_scroll_response_detected
        result = self._build_scroll_result(
            interaction_mode="scroll_into_view",
            panel_focus_succeeded=panel_focus_succeeded,
            interaction_verified=panel_focus_succeeded and panel_scroll_response_detected and not map_view_interference_detected,
            panel_scroll_response_detected=panel_scroll_response_detected,
            outer_page_scroll_detected=outer_page_scroll_detected,
            map_view_interference_detected=map_view_interference_detected,
            scroll_top=after_metrics.get("scroll_top", -1),
        )
        result.update(
            {
                "before_scroll_top": before_metrics.get("scroll_top", -1),
                "after_scroll_top": after_metrics.get("scroll_top", -1),
                "before_scroll_height": before_metrics.get("scroll_height", -1),
                "after_scroll_height": after_metrics.get("scroll_height", -1),
                "before_review_id_count": len(before_review_ids),
                "after_review_id_count": len(after_review_ids),
                "scroll_write_succeeded": panel_scroll_response_detected,
                "scroll_delta": max(0, after_metrics.get("scroll_top", -1) - before_metrics.get("scroll_top", -1)),
                "container_scroll_write_failed": not panel_scroll_response_detected,
                "step_count_in_round": 1,
                "step_targets": [after_metrics.get("scroll_top", -1)],
                "step_scroll_tops": [before_metrics.get("scroll_top", -1), after_metrics.get("scroll_top", -1)],
                "distance_to_bottom_before": self._distance_to_scroll_bottom(before_metrics),
                "distance_to_bottom_after": self._distance_to_scroll_bottom(after_metrics),
                "near_bottom_before": self._is_near_scroll_bottom(before_metrics),
                "near_bottom_after": self._is_near_scroll_bottom(after_metrics),
            }
        )
        return result

    def _scroll_with_action_chains(self, driver: webdriver.Chrome, reviews_panel: WebElement, last_card: WebElement | None) -> dict[str, Any]:
        before_metrics = self._get_scroll_metrics(driver, reviews_panel)
        before_window_scroll = self._get_window_scroll_y(driver)
        panel_focus_succeeded = self._focus_reviews_panel(driver, reviews_panel)
        before_review_ids = self._review_ids_in_container(reviews_panel)
        try:
            target = last_card or reviews_panel
            ActionChains(driver).move_to_element(target).pause(random.uniform(0.15, 0.35)).perform()
            time.sleep(random.uniform(0.35, 0.55))
            ActionChains(driver).scroll_by_amount(0, random.randint(180, 300)).pause(random.uniform(0.18, 0.3)).perform()
            time.sleep(random.uniform(0.3, 0.55))
        except WebDriverException:
            panel_focus_succeeded = False
        after_metrics = self._get_scroll_metrics(driver, reviews_panel)
        after_window_scroll = self._get_window_scroll_y(driver)
        after_review_ids = self._review_ids_in_container(reviews_panel)
        panel_scroll_response_detected = after_metrics.get("scroll_top", -1) > before_metrics.get("scroll_top", -1)
        outer_page_scroll_detected = after_window_scroll > before_window_scroll >= 0
        map_view_interference_detected = outer_page_scroll_detected and not panel_scroll_response_detected
        result = self._build_scroll_result(
            interaction_mode="wheel",
            panel_focus_succeeded=panel_focus_succeeded,
            interaction_verified=panel_focus_succeeded and panel_scroll_response_detected and not map_view_interference_detected,
            panel_scroll_response_detected=panel_scroll_response_detected,
            outer_page_scroll_detected=outer_page_scroll_detected,
            map_view_interference_detected=map_view_interference_detected,
            scroll_top=after_metrics.get("scroll_top", -1),
        )
        result.update(
            {
                "before_scroll_top": before_metrics.get("scroll_top", -1),
                "after_scroll_top": after_metrics.get("scroll_top", -1),
                "before_scroll_height": before_metrics.get("scroll_height", -1),
                "after_scroll_height": after_metrics.get("scroll_height", -1),
                "before_review_id_count": len(before_review_ids),
                "after_review_id_count": len(after_review_ids),
                "scroll_write_succeeded": panel_scroll_response_detected,
                "scroll_delta": max(0, after_metrics.get("scroll_top", -1) - before_metrics.get("scroll_top", -1)),
                "container_scroll_write_failed": not panel_scroll_response_detected,
                "step_count_in_round": 1,
                "step_targets": [after_metrics.get("scroll_top", -1)],
                "step_scroll_tops": [before_metrics.get("scroll_top", -1), after_metrics.get("scroll_top", -1)],
                "distance_to_bottom_before": self._distance_to_scroll_bottom(before_metrics),
                "distance_to_bottom_after": self._distance_to_scroll_bottom(after_metrics),
                "near_bottom_before": self._is_near_scroll_bottom(before_metrics),
                "near_bottom_after": self._is_near_scroll_bottom(after_metrics),
            }
        )
        return result

    def _scroll_with_hybrid(self, driver: webdriver.Chrome, reviews_panel: WebElement, last_card: WebElement | None) -> dict[str, Any]:
        before_metrics = self._get_scroll_metrics(driver, reviews_panel)
        before_window_scroll = self._get_window_scroll_y(driver)
        panel_focus_succeeded = self._focus_reviews_panel(driver, reviews_panel)
        try:
            if last_card is not None:
                driver.execute_script("arguments[0].scrollIntoView({block: 'end', inline: 'nearest'});", last_card)
                time.sleep(random.uniform(0.35, 0.6))
            try:
                ActionChains(driver).move_to_element(last_card or reviews_panel).pause(random.uniform(0.15, 0.35)).scroll_by_amount(0, random.randint(180, 320)).perform()
            except WebDriverException:
                driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollTop + arguments[1]; arguments[0].dispatchEvent(new Event('scroll')); arguments[0].dispatchEvent(new WheelEvent('wheel', {deltaY: arguments[1], bubbles: true}));",
                    reviews_panel,
                    random.randint(180, 320),
                )
            time.sleep(random.uniform(0.3, 0.55))
        except WebDriverException:
            panel_focus_succeeded = False
        after_metrics = self._get_scroll_metrics(driver, reviews_panel)
        after_window_scroll = self._get_window_scroll_y(driver)
        panel_scroll_response_detected = after_metrics.get("scroll_top", -1) > before_metrics.get("scroll_top", -1)
        outer_page_scroll_detected = after_window_scroll > before_window_scroll >= 0
        map_view_interference_detected = outer_page_scroll_detected and not panel_scroll_response_detected
        result = self._build_scroll_result(
            interaction_mode="hybrid",
            panel_focus_succeeded=panel_focus_succeeded,
            interaction_verified=panel_focus_succeeded and panel_scroll_response_detected and not map_view_interference_detected,
            panel_scroll_response_detected=panel_scroll_response_detected,
            outer_page_scroll_detected=outer_page_scroll_detected,
            map_view_interference_detected=map_view_interference_detected,
            scroll_top=after_metrics.get("scroll_top", -1),
        )
        result["lazy_load_strategy"] = "hybrid"
        result["strategy_attempt_index"] = 0
        return result

    def _safe_text(self, root: WebElement, selectors: list[str]) -> str | None:
        for selector in selectors:
            try:
                elements = root.find_elements(By.CSS_SELECTOR, selector)
            except (NoSuchElementException, StaleElementReferenceException):
                continue
            for element in elements:
                text = self._normalize_whitespace(element.text)
                if text:
                    return text
        return None

    def _safe_attribute(self, root: WebElement, selector: str, attribute: str) -> str | None:
        try:
            elements = root.find_elements(By.CSS_SELECTOR, selector)
        except (NoSuchElementException, StaleElementReferenceException):
            return None
        for element in elements:
            value = element.get_attribute(attribute)
            if value:
                return self._normalize_whitespace(value)
        return None

    def _estimate_review_date(self, review_date_text: str | None) -> str | None:
        if not review_date_text:
            return None

        normalized = review_date_text.strip()
        iso_match = re.search(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})", normalized)
        if iso_match:
            return iso_match.group(1).replace("/", "-")

        absolute_zh = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", normalized)
        if absolute_zh:
            year, month, day = absolute_zh.groups()
            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

        return None

    def _build_review_unique_key(
        self,
        *,
        place_id: str,
        raw_review_id: str | None,
        reviewer_name: str | None,
        star_rating: float | None,
        review_date_text: str | None,
        review_text: str | None,
    ) -> str:
        if raw_review_id:
            payload = f"{place_id}||review_id||{self._normalize_text(raw_review_id)}"
            return hashlib.sha256(payload.encode("utf-8")).hexdigest()

        fingerprint_parts = [
            place_id,
            self._normalize_text(reviewer_name),
            "" if star_rating is None else str(star_rating),
            self._normalize_text(review_date_text),
            self._normalize_text(review_text),
        ]
        payload = "||".join(fingerprint_parts)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _review_identity(self, record: ReviewRecord) -> str:
        review_id = (record.raw_review_metadata or {}).get("review_id")
        if review_id:
            return f"{record.place_id}::review_id::{review_id}"
        return f"{record.place_id}::review_key::{record.review_unique_key}"

    def _derive_place_id(self, *, source_url: str, place_name: str | None) -> str:
        if self.config.place_id_override:
            return self._slugify(self.config.place_id_override)

        slug = self._slugify(place_name or "google-place")
        if slug != "google-place":
            return slug

        google_place_token = self._extract_google_place_token(source_url)
        if google_place_token:
            return f"google-place-{google_place_token[:12]}"

        parsed = urlparse(source_url)
        source_basis = parsed.path or source_url
        raw = f"{place_name or 'unknown-place'}::{source_basis}"
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
        return f"google-place-{digest}"

    def _extract_google_place_token(self, source_url: str) -> str | None:
        decoded_url = unquote(source_url)
        match = re.search(r"!1s(0x[0-9a-f]+:0x[0-9a-f]+)", decoded_url, flags=re.IGNORECASE)
        if match:
            return self._slugify(match.group(1).replace(":", "-"))

        query = parse_qs(urlparse(source_url).query)
        place_id_values = query.get("place_id") or query.get("q")
        if place_id_values:
            return self._slugify(place_id_values[0])
        return None

    def _slugify(self, value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value)
        normalized = normalized.lower().strip()
        normalized = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "-", normalized)
        normalized = re.sub(r"-{2,}", "-", normalized)
        return normalized.strip("-") or "google-place"

    def _normalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        normalized = unicodedata.normalize("NFKC", value)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip().lower()

    def _normalize_whitespace(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = unicodedata.normalize("NFKC", value)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    def _save_debug_artifacts(self, driver: webdriver.Chrome, paths: dict[str, Path], reason: str) -> None:
        base_name = f"debug_{self.scrape_run_id}_{reason}"
        html_path = paths["place_dir"] / f"{base_name}.html"
        screenshot_path = paths["place_dir"] / f"{base_name}.png"
        json_path = paths["place_dir"] / f"{base_name}.json"

        try:
            html_path.write_text(driver.page_source, encoding="utf-8")
        except Exception:
            pass
        try:
            driver.save_screenshot(str(screenshot_path))
        except Exception:
            pass
        try:
            json_path.write_text(json.dumps(self.debug_info, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _record_event(self, message: str) -> None:
        timestamp = datetime.now(UTC).isoformat()
        self.debug_info.setdefault("events", []).append({"time": timestamp, "message": message})

    def _debug_print(self, message: str) -> None:
        print(f"[debug] {message}", flush=True)
        self._record_event(message)

    def _sleep_action(self) -> None:
        time.sleep(random.uniform(self.config.action_pause_min_seconds, self.config.action_pause_max_seconds))

    def _sleep_scroll(self) -> None:
        time.sleep(random.uniform(self.config.scroll_pause_min_seconds, self.config.scroll_pause_max_seconds))
