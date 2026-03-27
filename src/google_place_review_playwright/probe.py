from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

from playwright.sync_api import BrowserContext
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

VERIFIED_PANE_SELECTOR = "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde"
REVIEW_CARD_SELECTOR = "[data-review-id]"
MAPS_HOME_URL = "https://www.google.com/maps?hl=zh-TW"


@dataclass(slots=True)
class PlaywrightProbeConfig:
    target_url: str | None = None
    entry_mode: str = "direct_url"
    search_query: str | None = None
    place_name_hint: str | None = None
    headless: bool = True
    max_rounds: int = 6
    output_dir: Path = Path("data/playwright")
    debug_save_html: bool = False
    place_slug_override: str | None = None
    use_system_chrome: bool = True
    profile_dir: Path = Path("data/playwright/profile/manual_google_maps")
    manual_ready_timeout: int = 120
    expect_reviews_tab: bool = False
    stable_round_threshold: int = 3
    round_wait_ms: int = 1200


@dataclass(slots=True)
class PlaywrightProbeResult:
    place_slug: str
    run_id: str
    output_json_path: Path
    pane_found: bool
    review_card_count: int
    entry_verdict: str
    strategy_results: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PlaywrightProbe:
    def __init__(self, config: PlaywrightProbeConfig) -> None:
        self.config = config
        self.run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.debug_info: dict[str, Any] = {
            "framework": "playwright",
            "entry_mode": config.entry_mode,
            "target_url": config.target_url,
            "search_query": config.search_query,
            "place_name_hint": config.place_name_hint,
            "profile_dir": str(config.profile_dir),
            "manual_handoff_expected": config.entry_mode == "attach_existing_reviews_page",
            "manual_ready_timeout": config.manual_ready_timeout,
            "expect_reviews_tab": config.expect_reviews_tab,
            "verified_scroll_selector": VERIFIED_PANE_SELECTOR,
            "review_card_selector": REVIEW_CARD_SELECTOR,
            "events": [],
            "strategy_results": {},
            "tab_candidates": [],
        }

    def run(self) -> PlaywrightProbeResult:
        place_slug = self._resolve_place_slug()
        paths = self._ensure_probe_paths(place_slug)
        output_json_path = paths["runs_dir"] / f"probe_{self.run_id}.json"
        output_png_path = paths["runs_dir"] / f"probe_{self.run_id}.png"
        output_html_path = paths["runs_dir"] / f"probe_{self.run_id}.html"
        pre_entry_png_path = paths["runs_dir"] / f"probe_{self.run_id}_pre_entry.png"
        pre_entry_html_path = paths["runs_dir"] / f"probe_{self.run_id}_pre_entry.html"
        post_tab_png_path = paths["runs_dir"] / f"probe_{self.run_id}_post_tab.png"
        post_tab_html_path = paths["runs_dir"] / f"probe_{self.run_id}_post_tab.html"

        with sync_playwright() as playwright:
            context = self._launch_context(playwright)
            page = self._get_or_create_page(context)
            try:
                self._enter_page(page)
                self._save_page_artifacts(page, pre_entry_png_path, pre_entry_html_path)
                self.debug_info["pre_entry_screenshot"] = str(pre_entry_png_path)
                if self.config.debug_save_html:
                    self.debug_info["pre_entry_html"] = str(pre_entry_html_path)

                pane, entry_mode = self._enter_reviews_context(page)
                self.debug_info["reviews_entry_mode"] = entry_mode
                self.debug_info["pane_found"] = pane is not None

                self._save_page_artifacts(page, post_tab_png_path, post_tab_html_path)
                self.debug_info["post_probe_screenshot"] = str(post_tab_png_path)
                if self.config.debug_save_html:
                    self.debug_info["post_probe_html"] = str(post_tab_html_path)

                if pane is None:
                    self.debug_info["entry_verdict"] = "failed_to_enter_reviews_context"
                    self.debug_info["final_stop_reason"] = "failed_to_enter_reviews_context"
                    self._save_debug_artifacts(page, output_json_path, output_png_path, output_html_path)
                    return PlaywrightProbeResult(
                        place_slug=place_slug,
                        run_id=self.run_id,
                        output_json_path=output_json_path,
                        pane_found=False,
                        review_card_count=0,
                        entry_verdict="failed_to_enter_reviews_context",
                        strategy_results={},
                    )

                pane_metrics = self._pane_metrics(pane)
                review_card_count = self._review_card_count(pane)
                last_review_id_before = self._last_review_id(pane)
                entry_verdict = self._determine_entry_verdict(pane_metrics, review_card_count)
                self.debug_info["entry_verdict"] = entry_verdict
                self.debug_info["pane_selector_hit"] = VERIFIED_PANE_SELECTOR
                self.debug_info["overflow_y"] = pane_metrics["overflow_y"]
                self.debug_info["scroll_top"] = pane_metrics["scroll_top"]
                self.debug_info["scroll_height"] = pane_metrics["scroll_height"]
                self.debug_info["client_height"] = pane_metrics["client_height"]
                self.debug_info["review_card_count_before"] = review_card_count
                self.debug_info["last_review_id_before"] = last_review_id_before
                self._record_event(f"entry_verdict:{entry_verdict}")

                strategy_results: dict[str, Any] = {}
                if entry_verdict != "failed_to_enter_reviews_context":
                    strategy_results = self._run_strategy_rounds(page, pane)
                self.debug_info["strategy_results"] = strategy_results

                final_metrics = self._pane_metrics(pane)
                final_card_count = self._review_card_count(pane)
                self.debug_info["review_card_count_after"] = final_card_count
                self.debug_info["last_review_id_after"] = self._last_review_id(pane)
                self.debug_info["final_scroll_top"] = final_metrics["scroll_top"]
                self.debug_info["final_scroll_height"] = final_metrics["scroll_height"]

                self._save_debug_artifacts(page, output_json_path, output_png_path, output_html_path)
                return PlaywrightProbeResult(
                    place_slug=place_slug,
                    run_id=self.run_id,
                    output_json_path=output_json_path,
                    pane_found=True,
                    review_card_count=final_card_count,
                    entry_verdict=entry_verdict,
                    strategy_results=strategy_results,
                )
            except Exception as exc:
                self.debug_info["entry_verdict"] = "failed_to_enter_reviews_context"
                self.debug_info["final_stop_reason"] = exc.__class__.__name__
                self.debug_info["exception"] = {
                    "type": exc.__class__.__name__,
                    "message": str(exc),
                }
                self._record_event(f"probe_exception:{exc.__class__.__name__}")
                self._save_debug_artifacts(page, output_json_path, output_png_path, output_html_path)
                return PlaywrightProbeResult(
                    place_slug=place_slug,
                    run_id=self.run_id,
                    output_json_path=output_json_path,
                    pane_found=False,
                    review_card_count=0,
                    entry_verdict="failed_to_enter_reviews_context",
                    strategy_results={},
                )
            finally:
                context.close()

    def _launch_context(self, playwright: Any) -> BrowserContext:
        if self.config.entry_mode == "attach_existing_reviews_page":
            self.config.profile_dir.mkdir(parents=True, exist_ok=True)
            launch_kwargs: dict[str, Any] = {
                "user_data_dir": str(self.config.profile_dir),
                "headless": self.config.headless,
                "args": ["--disable-dev-shm-usage", "--no-sandbox"],
                "viewport": {"width": 1440, "height": 1080},
            }
            if self.config.use_system_chrome:
                launch_kwargs["channel"] = "chrome"
                self._record_event("browser_launch:persistent_system_chrome_profile")
            else:
                self._record_event("browser_launch:persistent_playwright_profile")
            return playwright.chromium.launch_persistent_context(**launch_kwargs)

        browser = self._launch_browser(playwright)
        context = browser.new_context(viewport={"width": 1440, "height": 1080})
        self._record_event("browser_context:ephemeral")
        return context

    def _get_or_create_page(self, context: BrowserContext) -> Page:
        if context.pages:
            page = context.pages[0]
            page.bring_to_front()
            return page
        return context.new_page()

    def _enter_page(self, page: Page) -> None:
        if self.config.entry_mode == "attach_existing_reviews_page":
            self._enter_via_manual_handoff(page)
            return
        if self.config.entry_mode == "search_then_click_place":
            self._enter_via_search_then_click_place(page)
            return
        if not self.config.target_url:
            raise ValueError("direct_url 模式需要提供 target_url")
        page.goto(self.config.target_url, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(2500)
        self._record_event("page_loaded:direct_url")

    def _enter_via_manual_handoff(self, page: Page) -> None:
        self._record_event("manual_handoff_started")
        if not page.url or page.url == "about:blank":
            page.goto(MAPS_HOME_URL, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(2000)
            self._record_event("manual_handoff_opened_maps_home")

        deadline = datetime.now(UTC).timestamp() + self.config.manual_ready_timeout
        while datetime.now(UTC).timestamp() < deadline:
            current_url = page.url
            current_title = page.title()
            if "google.com/maps" in current_url.lower():
                self.debug_info["current_page_url_before_probe"] = current_url
                self.debug_info["current_page_title_before_probe"] = current_title
                if self._review_card_count(page.locator("body")) > 0 or self._has_reviews_tab(page):
                    self._record_event("manual_handoff_completed")
                    page.wait_for_timeout(1000)
                    return
            page.wait_for_timeout(1000)
        self.debug_info["current_page_url_before_probe"] = page.url
        self.debug_info["current_page_title_before_probe"] = page.title()
        raise RuntimeError("等待手動準備評論頁逾時")

    def _enter_via_search_then_click_place(self, page: Page) -> None:
        query = (self.config.search_query or self.config.place_name_hint or "").strip()
        if not query:
            raise ValueError("search_then_click_place 模式需要提供 search_query 或 place_name_hint")
        page.goto(MAPS_HOME_URL, wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(2500)
        self._record_event("page_loaded:maps_home")
        search_box = self._wait_for_first(
            page,
            [
                "#searchboxinput",
                "input#searchboxinput",
                "input[name='q']",
                "input[role='combobox']",
                "input[aria-label*='搜尋']",
                "input[aria-label*='Search']",
                "input[placeholder*='搜尋']",
                "input[placeholder*='Search']",
            ],
            timeout=15000,
        )
        if search_box is None:
            self.debug_info["page_title_before_search_failure"] = page.title()
            self.debug_info["page_url_before_search_failure"] = page.url
            raise RuntimeError("找不到 Google Maps 搜尋框")
        search_box.click()
        search_box.fill(query)
        page.wait_for_timeout(300)
        search_box.press("Enter")
        page.wait_for_timeout(3500)
        self._record_event(f"search_submitted:{query}")

        if self._has_place_heading(page):
            self.debug_info["search_results_detected"] = False
            self.debug_info["selected_place_candidate"] = query
            self._record_event("search_resolved_directly_to_place")
            return

        candidate = self._select_search_result(page)
        self.debug_info["search_results_detected"] = candidate is not None
        if candidate is None:
            self._record_event("search_result_candidate_not_found")
            return
        self.debug_info["selected_place_candidate"] = candidate.get("text") or candidate.get("aria_label") or ""
        self._record_event(f"search_result_candidate_selected:{self.debug_info['selected_place_candidate']}")
        candidate["locator"].click(timeout=5000)
        page.wait_for_timeout(3500)
        self._record_event("search_result_clicked")

    def _select_search_result(self, page: Page) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        hint = (self.config.place_name_hint or self.config.search_query or "").strip().lower()
        selectors = [
            "a[href*='/place/']",
            "div[role='article'] a",
            "div[role='feed'] a",
        ]
        seen_keys: set[str] = set()
        for selector in selectors:
            locator = page.locator(selector)
            count = min(locator.count(), 12)
            for index in range(count):
                item = locator.nth(index)
                try:
                    text = (item.inner_text(timeout=1000) or "").strip()
                except Exception:
                    text = ""
                aria_label = item.get_attribute("aria-label") or ""
                href = item.get_attribute("href") or ""
                key = f"{text}|{aria_label}|{href}"
                if not key.strip("|") or key in seen_keys:
                    continue
                seen_keys.add(key)
                try:
                    visible = item.is_visible()
                except Exception:
                    visible = False
                score = 0
                haystack = f"{text} {aria_label}".lower()
                if hint and hint in haystack:
                    score += 10
                if "/place/" in href:
                    score += 4
                if visible:
                    score += 2
                candidates.append(
                    {
                        "text": text,
                        "aria_label": aria_label,
                        "href": href,
                        "visible": visible,
                        "score": score,
                        "locator": item,
                    }
                )
        candidates.sort(key=lambda item: item["score"], reverse=True)
        if candidates:
            self.debug_info["search_result_candidates"] = [
                {
                    "text": item["text"],
                    "aria_label": item["aria_label"],
                    "href": item["href"],
                    "visible": item["visible"],
                    "score": item["score"],
                }
                for item in candidates[:8]
            ]
            return candidates[0]
        return None

    def _enter_reviews_context(self, page: Page) -> tuple[Locator | None, str]:
        self.debug_info["current_page_url_before_probe"] = page.url
        self.debug_info["current_page_title_before_probe"] = page.title()
        pane = self._find_verified_pane(page)
        if pane is not None:
            self.debug_info["manual_reviews_tab_already_open"] = True
            self._record_event("reviews_entry_detected:direct_pane")
            return pane, "direct_pane"

        self.debug_info["manual_reviews_tab_already_open"] = False
        if self.config.expect_reviews_tab:
            self._record_event("expected_reviews_tab_but_pane_missing")

        self._record_event("reviews_context_not_found_before_tab_click")
        tab_candidates = self._reviews_tab_candidates(page)
        self.debug_info["tab_candidates"] = [
            {k: v for k, v in candidate.items() if k != "locator"}
            for candidate in tab_candidates
        ]
        self.debug_info["tab_candidate_count"] = len(tab_candidates)
        selected = tab_candidates[0] if tab_candidates else None
        self.debug_info["selected_review_tab_candidate"] = (
            {k: v for k, v in selected.items() if k != "locator"} if selected else None
        )
        if selected is None:
            return None, "failed"

        self.debug_info["clicked_review_tab"] = False
        try:
            selected["locator"].click(timeout=5000)
            page.wait_for_timeout(2000)
            self.debug_info["clicked_review_tab"] = True
            self._record_event(f"reviews_tab_clicked:{selected['source']}")
        except PlaywrightTimeoutError:
            self._record_event(f"reviews_tab_click_failed:{selected['source']}:timeout")
            return None, "failed"
        except PlaywrightError as exc:
            self._record_event(f"reviews_tab_click_failed:{selected['source']}:{exc.__class__.__name__}")
            return None, "failed"

        pane = self._find_verified_pane(page)
        if pane is not None:
            self._record_event(f"reviews_entry_opened_by:{selected['source']}")
            return pane, f"clicked:{selected['source']}"
        return None, "failed"

    def _find_verified_pane(self, page: Page) -> Locator | None:
        locator = page.locator(VERIFIED_PANE_SELECTOR)
        try:
            locator.first.wait_for(state="attached", timeout=5000)
        except PlaywrightTimeoutError:
            return None
        for index in range(locator.count()):
            candidate = locator.nth(index)
            try:
                metrics = self._pane_metrics(candidate)
            except Exception:
                continue
            if metrics["overflow_y"] in {"auto", "scroll"} and metrics["scroll_height"] > metrics["client_height"] + 40:
                return candidate
        return None

    def _reviews_tab_candidates(self, page: Page) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        selectors = [
            ("hh2c6_review_tab", "button.hh2c6[role='tab']"),
            ("generic_role_tab", "[role='tab']"),
        ]
        for source, selector in selectors:
            locator = page.locator(selector)
            count = min(locator.count(), 10)
            for index in range(count):
                item = locator.nth(index)
                try:
                    text = (item.inner_text(timeout=1000) or "").strip()
                except Exception:
                    text = ""
                aria_label = item.get_attribute("aria-label") or ""
                role = item.get_attribute("role") or ""
                class_name = item.get_attribute("class") or ""
                data_tab_index = item.get_attribute("data-tab-index") or ""
                try:
                    visible = item.is_visible()
                except Exception:
                    visible = False
                combined = f"{text} {aria_label}".lower()
                if "write a review" in combined or "撰寫評論" in combined:
                    continue
                key = f"{source}|{text}|{aria_label}|{data_tab_index}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                score = 0
                if "評論" in combined or "reviews" in combined or "review" in combined:
                    score += 10
                if role == "tab":
                    score += 3
                if data_tab_index == "2":
                    score += 2
                if "hh2c6" in class_name:
                    score += 2
                if visible:
                    score += 1
                candidates.append(
                    {
                        "source": source,
                        "text": text,
                        "aria_label": aria_label,
                        "role": role,
                        "class_name": class_name,
                        "data_tab_index": data_tab_index,
                        "visible": visible,
                        "score": score,
                        "locator": item,
                    }
                )
        candidates.sort(key=lambda item: item["score"], reverse=True)
        return candidates

    def _has_reviews_tab(self, page: Page) -> bool:
        return len(self._reviews_tab_candidates(page)) > 0

    def _run_strategy_rounds(self, page: Page, pane: Locator) -> dict[str, Any]:
        strategy_funcs = [
            ("pane_step_scroll", self._strategy_pane_step_scroll),
            ("last_card_into_view", self._strategy_last_card_into_view),
            ("mouse_wheel_over_pane", self._strategy_mouse_wheel_over_pane),
        ]
        results: dict[str, Any] = {}
        for name, func in strategy_funcs:
            before_metrics = self._pane_metrics(pane)
            before_count = self._review_card_count(pane)
            before_last_id = self._last_review_id(pane)
            action_result = func(page, pane)
            page.wait_for_timeout(1200)
            after_metrics = self._pane_metrics(pane)
            after_count = self._review_card_count(pane)
            after_last_id = self._last_review_id(pane)
            results[name] = {
                "action_result": action_result,
                "review_card_count_before": before_count,
                "review_card_count_after": after_count,
                "last_review_id_before": before_last_id,
                "last_review_id_after": after_last_id,
                "scroll_top_before": before_metrics["scroll_top"],
                "scroll_top_after": after_metrics["scroll_top"],
                "scroll_height_before": before_metrics["scroll_height"],
                "scroll_height_after": after_metrics["scroll_height"],
                "review_count_delta": after_count - before_count,
                "scroll_height_delta": after_metrics["scroll_height"] - before_metrics["scroll_height"],
                "triggered_growth": (
                    after_count > before_count
                    or after_metrics["scroll_height"] > before_metrics["scroll_height"]
                    or (after_last_id and after_last_id != before_last_id)
                ),
            }
            self._record_event(
                f"strategy:{name}:review_count_delta={results[name]['review_count_delta']}:scroll_height_delta={results[name]['scroll_height_delta']}"
            )
        return results

    def _strategy_pane_step_scroll(self, page: Page, pane: Locator) -> dict[str, Any]:
        metrics = self._pane_metrics(pane)
        step = max(220, int(metrics["client_height"] * 0.4))
        step_scroll_tops: list[int] = []
        for _ in range(min(3, self.config.max_rounds)):
            pane.evaluate(
                """(node, stepPx) => {
                    node.scrollTop = Math.min(node.scrollTop + stepPx, node.scrollHeight);
                    node.dispatchEvent(new Event('scroll', { bubbles: true }));
                    node.dispatchEvent(new WheelEvent('wheel', { deltaY: stepPx, bubbles: true, cancelable: true }));
                }""",
                step,
            )
            page.wait_for_timeout(700)
            step_scroll_tops.append(self._pane_metrics(pane)["scroll_top"])
        return {"step": step, "step_scroll_tops": step_scroll_tops}

    def _strategy_last_card_into_view(self, page: Page, pane: Locator) -> dict[str, Any]:
        last_card = pane.locator(REVIEW_CARD_SELECTOR).last
        if last_card.count() <= 0:
            return {"scrolled": False}
        last_card.scroll_into_view_if_needed(timeout=3000)
        page.wait_for_timeout(800)
        pane.evaluate(
            """(node) => {
                node.scrollTop = Math.min(node.scrollTop + Math.floor(node.clientHeight * 0.25), node.scrollHeight);
                node.dispatchEvent(new Event('scroll', { bubbles: true }));
            }"""
        )
        page.wait_for_timeout(800)
        return {"scrolled": True}

    def _strategy_mouse_wheel_over_pane(self, page: Page, pane: Locator) -> dict[str, Any]:
        box = pane.bounding_box()
        if not box:
            return {"wheel_used": False}
        page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
        page.mouse.wheel(0, max(240, box["height"] * 0.5))
        page.wait_for_timeout(900)
        return {"wheel_used": True}

    def _pane_metrics(self, pane: Locator) -> dict[str, Any]:
        return pane.evaluate(
            """(node) => ({
                scroll_top: Math.floor(node.scrollTop || 0),
                scroll_height: Math.floor(node.scrollHeight || 0),
                client_height: Math.floor(node.clientHeight || 0),
                overflow_y: window.getComputedStyle(node).overflowY || '',
            })"""
        )

    def _review_card_count(self, scope: Locator) -> int:
        try:
            return scope.locator(REVIEW_CARD_SELECTOR).count()
        except Exception:
            return 0

    def _last_review_id(self, pane: Locator) -> str | None:
        try:
            card = pane.locator(REVIEW_CARD_SELECTOR).last
            if card.count() <= 0:
                return None
            return card.get_attribute("data-review-id") or None
        except Exception:
            return None

    def _determine_entry_verdict(self, pane_metrics: dict[str, Any], card_count: int) -> str:
        if (
            pane_metrics["overflow_y"] in {"auto", "scroll"}
            and pane_metrics["scroll_height"] > pane_metrics["client_height"] + 40
            and card_count > 0
        ):
            return "entered_full_review_list"
        if pane_metrics["overflow_y"] in {"auto", "scroll"}:
            return "entered_embedded_review_section"
        return "failed_to_enter_reviews_context"

    def _wait_for_first(self, page: Page, selectors: list[str], timeout: int) -> Locator | None:
        deadline = datetime.now(UTC).timestamp() + timeout / 1000
        while datetime.now(UTC).timestamp() < deadline:
            for selector in selectors:
                locator = page.locator(selector)
                if locator.count() <= 0:
                    continue
                try:
                    if locator.first.is_visible():
                        return locator.first
                except Exception:
                    continue
            page.wait_for_timeout(300)
        return None

    def _has_place_heading(self, page: Page) -> bool:
        selectors = [
            "h1.DUwDvf",
            "h1[class*='DUwDvf']",
            "div[role='main'] h1",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            if locator.count() <= 0:
                continue
            try:
                if locator.first.is_visible():
                    return True
            except Exception:
                continue
        return False

    def _save_debug_artifacts(self, page: Page, json_path: Path, png_path: Path, html_path: Path) -> None:
        json_path.write_text(json.dumps(self.debug_info, ensure_ascii=False, indent=2), encoding="utf-8")
        self._save_page_artifacts(page, png_path, html_path)

    def _save_page_artifacts(self, page: Page, png_path: Path, html_path: Path) -> None:
        try:
            page.screenshot(path=str(png_path), full_page=True)
        except Exception:
            pass
        if self.config.debug_save_html:
            try:
                html_path.write_text(page.content(), encoding="utf-8")
            except Exception:
                pass

    def _resolve_place_slug(self) -> str:
        if self.config.place_slug_override:
            return self.config.place_slug_override
        if self.config.place_name_hint:
            return self._slugify(self.config.place_name_hint)
        if self.config.search_query:
            return self._slugify(self.config.search_query)
        if self.config.target_url:
            return self._derive_place_slug(self.config.target_url)
        return "playwright-probe"

    def _derive_place_slug(self, url: str) -> str:
        match = re.search(r"/place/([^/@]+)", url)
        raw = match.group(1) if match else quote(url, safe="")[:40]
        return self._slugify(raw.replace("+", " "))

    def _slugify(self, text: str) -> str:
        normalized = unicodedata.normalize("NFKC", text)
        normalized = re.sub(r"\s+", "-", normalized.strip().lower())
        normalized = re.sub(r"[^a-z0-9\u4e00-\u9fff_-]+", "-", normalized)
        normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
        return normalized or "playwright-probe"

    def _ensure_probe_paths(self, place_slug: str) -> dict[str, Path]:
        place_dir = self.config.output_dir / place_slug
        runs_dir = place_dir / "runs"
        place_dir.mkdir(parents=True, exist_ok=True)
        runs_dir.mkdir(parents=True, exist_ok=True)
        return {"place_dir": place_dir, "runs_dir": runs_dir}

    def _launch_browser(self, playwright: Any):
        launch_kwargs = {
            "headless": self.config.headless,
            "args": ["--disable-dev-shm-usage", "--no-sandbox"],
        }
        if self.config.use_system_chrome:
            try:
                self._record_event("browser_launch:system_chrome_channel")
                return playwright.chromium.launch(channel="chrome", **launch_kwargs)
            except PlaywrightError as exc:
                self._record_event(f"browser_launch:system_chrome_channel_failed:{exc.__class__.__name__}")
        self._record_event("browser_launch:playwright_chromium")
        return playwright.chromium.launch(**launch_kwargs)

    def _record_event(self, message: str) -> None:
        self.debug_info.setdefault("events", []).append(
            {"time": datetime.now(UTC).isoformat(), "message": message}
        )
