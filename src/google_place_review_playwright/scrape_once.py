from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from playwright.sync_api import ElementHandle
from playwright.sync_api import Locator

from google_place_review.models import ReviewRecord
from google_place_review.storage import write_jsonl
from google_place_review_playwright.probe import PlaywrightProbe
from google_place_review_playwright.probe import PlaywrightProbeConfig


@dataclass(slots=True)
class PlaywrightScrapeOnceResult:
    place_id: str
    place_name: str
    scrape_run_id: str
    review_count: int
    output_jsonl_path: Path
    output_debug_json_path: Path


class PlaywrightReviewScrapeOnce(PlaywrightProbe):
    def __init__(self, config: PlaywrightProbeConfig, *, place_id: str = "rebirth", place_name: str = "Rebirth") -> None:
        super().__init__(config)
        self.place_id = place_id
        self.place_name = place_name
        self.scrape_run_id = self.run_id
        self.debug_info["place_id"] = place_id
        self.debug_info["place_name"] = place_name

    def run(self) -> PlaywrightScrapeOnceResult:
        place_slug = self.place_id
        paths = self._ensure_probe_paths(place_slug)
        output_jsonl_path = paths["runs_dir"] / f"reviews_{self.scrape_run_id}.jsonl"
        output_debug_json_path = paths["runs_dir"] / f"debug_{self.scrape_run_id}.json"
        output_debug_png_path = paths["runs_dir"] / f"debug_{self.scrape_run_id}.png"
        output_debug_html_path = paths["runs_dir"] / f"debug_{self.scrape_run_id}.html"
        pre_entry_png_path = paths["runs_dir"] / f"debug_{self.scrape_run_id}_pre_entry.png"
        pre_entry_html_path = paths["runs_dir"] / f"debug_{self.scrape_run_id}_pre_entry.html"

        with self._playwright_context() as (context, page):
            try:
                self._enter_page(page)
                self._save_page_artifacts(page, pre_entry_png_path, pre_entry_html_path)
                self.debug_info["pre_entry_screenshot"] = str(pre_entry_png_path)
                if self.config.debug_save_html:
                    self.debug_info["pre_entry_html"] = str(pre_entry_html_path)

                pane, entry_mode = self._enter_reviews_context(page)
                self.debug_info["reviews_entry_mode"] = entry_mode
                self.debug_info["pane_found"] = pane is not None

                if pane is None:
                    self.debug_info["entry_verdict"] = "failed_to_enter_reviews_context"
                    self.debug_info["final_stop_reason"] = "failed_to_enter_reviews_context"
                    self._save_debug_artifacts(page, output_debug_json_path, output_debug_png_path, output_debug_html_path)
                    return PlaywrightScrapeOnceResult(
                        place_id=self.place_id,
                        place_name=self.place_name,
                        scrape_run_id=self.scrape_run_id,
                        review_count=0,
                        output_jsonl_path=output_jsonl_path,
                        output_debug_json_path=output_debug_json_path,
                    )

                pane_metrics = self._pane_metrics(pane)
                initial_card_count = self._review_card_count(pane)
                entry_verdict = self._determine_entry_verdict(pane_metrics, initial_card_count)
                self.debug_info["entry_verdict"] = entry_verdict
                self.debug_info["initial_review_card_count"] = initial_card_count
                self.debug_info["pane_selector_hit"] = self.debug_info.get("verified_scroll_selector")
                if entry_verdict == "failed_to_enter_reviews_context":
                    self._save_debug_artifacts(page, output_debug_json_path, output_debug_png_path, output_debug_html_path)
                    return PlaywrightScrapeOnceResult(
                        place_id=self.place_id,
                        place_name=self.place_name,
                        scrape_run_id=self.scrape_run_id,
                        review_count=0,
                        output_jsonl_path=output_jsonl_path,
                        output_debug_json_path=output_debug_json_path,
                    )

                source_url = page.url
                google_place_token = self._extract_google_place_token(source_url)
                self.debug_info["source_url"] = source_url
                self.debug_info["google_place_token"] = google_place_token

                records, round_logs = self._collect_reviews_until_stable(page=page, pane=pane, source_url=source_url)
                self.debug_info["round_logs"] = round_logs
                self.debug_info["strategy_sequence"] = ["last_card_into_view", "pane_step_scroll", "mouse_wheel_over_pane"]
                self.debug_info["cards_collected_total"] = len(records)
                self.debug_info["final_review_card_count"] = self._review_card_count(pane)
                self.debug_info["output_jsonl_path"] = str(output_jsonl_path)

                write_jsonl(output_jsonl_path, records)
                self._save_debug_artifacts(page, output_debug_json_path, output_debug_png_path, output_debug_html_path)
                return PlaywrightScrapeOnceResult(
                    place_id=self.place_id,
                    place_name=self.place_name,
                    scrape_run_id=self.scrape_run_id,
                    review_count=len(records),
                    output_jsonl_path=output_jsonl_path,
                    output_debug_json_path=output_debug_json_path,
                )
            except Exception as exc:
                self.debug_info["entry_verdict"] = "failed_to_enter_reviews_context"
                self.debug_info["final_stop_reason"] = exc.__class__.__name__
                self.debug_info["exception"] = {"type": exc.__class__.__name__, "message": str(exc)}
                self._record_event(f"scrape_exception:{exc.__class__.__name__}")
                self._save_debug_artifacts(page, output_debug_json_path, output_debug_png_path, output_debug_html_path)
                raise

    def _playwright_context(self):
        from contextlib import contextmanager
        from playwright.sync_api import sync_playwright

        @contextmanager
        def manager():
            with sync_playwright() as playwright:
                context = self._launch_context(playwright)
                page = self._get_or_create_page(context)
                try:
                    yield context, page
                finally:
                    context.close()

        return manager()

    def _collect_reviews_until_stable(self, *, page: Any, pane: Locator, source_url: str) -> tuple[list[ReviewRecord], list[dict[str, Any]]]:
        records_by_identity: dict[str, ReviewRecord] = {}
        round_logs: list[dict[str, Any]] = []
        stable_rounds = 0

        initial_added = self._collect_visible_cards_into_store(
            pane=pane,
            source_url=source_url,
            records_by_identity=records_by_identity,
        )
        self._record_event(f"initial_cards_collected:{initial_added}")

        for round_index in range(1, self.config.max_rounds + 1):
            before_count = self._review_card_count(pane)
            before_height = self._pane_metrics(pane)["scroll_height"]
            before_last_id = self._last_review_id(pane)

            round_log: dict[str, Any] = {
                "round_index": round_index,
                "before_review_card_count": before_count,
                "before_scroll_height": before_height,
                "before_last_review_id": before_last_id,
                "strategies": [],
            }

            growth_detected = False
            for strategy_name, strategy_func in [
                ("last_card_into_view", self._strategy_last_card_into_view),
                ("pane_step_scroll", self._strategy_pane_step_scroll),
                ("mouse_wheel_over_pane", self._strategy_mouse_wheel_over_pane),
            ]:
                action_result = strategy_func(page, pane)
                page.wait_for_timeout(self.config.round_wait_ms)
                newly_added = self._collect_visible_cards_into_store(
                    pane=pane,
                    source_url=source_url,
                    records_by_identity=records_by_identity,
                )
                after_metrics = self._pane_metrics(pane)
                after_count = self._review_card_count(pane)
                after_last_id = self._last_review_id(pane)
                strategy_log = {
                    "strategy_name": strategy_name,
                    "action_result": action_result,
                    "new_records_added": newly_added,
                    "after_review_card_count": after_count,
                    "after_scroll_height": after_metrics["scroll_height"],
                    "after_scroll_top": after_metrics["scroll_top"],
                    "after_last_review_id": after_last_id,
                }
                round_log["strategies"].append(strategy_log)
                if newly_added > 0 or after_count > before_count or after_metrics["scroll_height"] > before_height or (
                    after_last_id and after_last_id != before_last_id
                ):
                    growth_detected = True
                    before_count = after_count
                    before_height = after_metrics["scroll_height"]
                    before_last_id = after_last_id
                    if strategy_name == "last_card_into_view":
                        break

            round_log["records_collected_total"] = len(records_by_identity)
            round_log["growth_detected"] = growth_detected
            round_logs.append(round_log)
            if growth_detected:
                stable_rounds = 0
            else:
                stable_rounds += 1
                if stable_rounds >= self.config.stable_round_threshold:
                    self._record_event("stop_reason:stable_no_growth")
                    break

        records = list(records_by_identity.values())
        return records, round_logs

    def _collect_visible_cards_into_store(
        self,
        *,
        pane: Locator,
        source_url: str,
        records_by_identity: dict[str, ReviewRecord],
    ) -> int:
        added = 0
        self._expand_visible_review_bodies(pane)
        card_handles = pane.locator(self.debug_info["review_card_selector"]).element_handles()
        for card in card_handles:
            try:
                self._expand_review_card(card)
                record = self._parse_review_card_to_record(card=card, source_url=source_url)
            except Exception:
                continue
            if record is None:
                continue
            identity = self._record_identity(record)
            if identity in records_by_identity:
                continue
            records_by_identity[identity] = record
            added += 1
        return added

    def _expand_visible_review_bodies(self, pane: Locator) -> None:
        card_handles = pane.locator(self.debug_info["review_card_selector"]).element_handles()
        expanded = 0
        for card in card_handles[:30]:
            try:
                expanded += self._expand_review_card(card)
            except Exception:
                continue
        if expanded:
            self._record_event(f"playwright_expanded_more_buttons:{expanded}")

    def _expand_review_card(self, card: Locator | ElementHandle) -> int:
        return card.evaluate(
            """(node) => {
                const selectors = [
                    'button.w8nwRe',
                    'button[jsaction*="expandReview"]',
                    'button[aria-label*="顯示更多"]',
                    'button[aria-label*="更多"]',
                    'button[aria-label*="Show more"]',
                ];
                const seen = new Set();
                let clicked = 0;
                for (const selector of selectors) {
                    const buttons = Array.from(node.querySelectorAll(selector));
                    for (const button of buttons) {
                        const ariaLabel = button.getAttribute('aria-label') || '';
                        const text = (button.innerText || button.textContent || '').replace(/\\s+/g, ' ').trim();
                        const signature = `${selector}||${ariaLabel}||${text}`;
                        if (seen.has(signature)) continue;
                        seen.add(signature);
                        if (button.getAttribute('aria-expanded') === 'true') continue;
                        const labelText = `${ariaLabel} ${text}`.toLowerCase();
                        if (!['顯示更多', '更多', 'show more', 'more'].some((token) => labelText.includes(token))) {
                            continue;
                        }
                        button.click();
                        clicked += 1;
                    }
                }
                return clicked;
            }"""
        )

    def _parse_review_card_to_record(self, *, card: Locator | ElementHandle, source_url: str) -> ReviewRecord | None:
        payload = self._extract_card_payload(card)
        reviewer_name = payload.get("reviewer_name")
        review_text = payload.get("review_text")
        if not reviewer_name and not review_text:
            return None

        review_id = payload.get("review_id")
        review_date_text = payload.get("review_date_text")
        star_rating = payload.get("star_rating")
        review_unique_key = self._build_review_unique_key(
            place_id=self.place_id,
            raw_review_id=review_id,
            reviewer_name=reviewer_name,
            star_rating=star_rating,
            review_date_text=review_date_text,
            review_text=review_text,
        )
        raw_review_metadata = {
            "review_id": review_id,
            "relative_date_text": review_date_text,
            "google_place_token": self.debug_info.get("google_place_token"),
        }
        return ReviewRecord(
            place_id=self.place_id,
            place_name=self.place_name,
            source_url=source_url,
            reviewer_name=reviewer_name,
            review_text=review_text,
            star_rating=star_rating,
            review_date_text=review_date_text,
            owner_response_text=payload.get("owner_response_text"),
            owner_response_date=payload.get("owner_response_date"),
            scraped_at=datetime.now(UTC).isoformat(),
            review_unique_key=review_unique_key,
            review_date_estimated=None,
            scrape_run_id=self.scrape_run_id,
            review_language=None,
            reviewer_profile_url=payload.get("reviewer_profile_url"),
            likes_count=payload.get("likes_count"),
            raw_review_metadata=raw_review_metadata,
        )

    def _extract_card_payload(self, card: Locator | ElementHandle) -> dict[str, Any]:
        return card.evaluate(
            """(node) => {
                const cleanText = (value) => {
                    if (!value) return null;
                    const normalized = value.replace(/\\s+/g, ' ').trim();
                    return normalized || null;
                };
                const textOf = (selectors) => {
                    for (const selector of selectors) {
                        const el = node.querySelector(selector);
                        if (!el) continue;
                        const value = cleanText(el.innerText || el.textContent || '');
                        if (value) return value;
                    }
                    return null;
                };
                const attrOf = (selector, attr) => {
                    const el = node.querySelector(selector);
                    return el ? el.getAttribute(attr) : null;
                };
                const expandedTexts = [];
                const containers = Array.from(node.querySelectorAll('.MyEned, .review-full-text, [data-review-text], [id^=\"ucc-\"]'));
                for (const container of containers) {
                    const clone = container.cloneNode(true);
                    clone.querySelectorAll('button, [role=\"button\"]').forEach((item) => item.remove());
                    const value = cleanText(clone.innerText || clone.textContent || '');
                    if (value) expandedTexts.push(value);
                }
                const reviewText = expandedTexts.sort((a, b) => b.length - a.length)[0] || null;
                const ratingSources = Array.from(node.querySelectorAll('.kvMYJc, span[role=\"img\"], [aria-label*=\"顆星\"], [aria-label*=\"star\"]'));
                let starRating = null;
                for (const el of ratingSources) {
                    const label = el.getAttribute('aria-label') || el.innerText || '';
                    const match = label.match(/([0-9]+(?:\\.[0-9]+)?)/);
                    if (match) {
                        starRating = Number(match[1]);
                        break;
                    }
                }
                const likeSources = Array.from(node.querySelectorAll('button, span'));
                let likesCount = null;
                for (const el of likeSources) {
                    const label = [el.getAttribute('aria-label') || '', el.getAttribute('title') || '', el.innerText || ''].join(' ');
                    const match = label.match(/([0-9]+)\\s*(?:人喜歡|人按讚|likes?)/i);
                    if (match) {
                        likesCount = Number(match[1]);
                        break;
                    }
                }
                return {
                    review_id: node.getAttribute('data-review-id'),
                    reviewer_name: textOf(['.d4r55', '.TSUbDb', 'div.d4r55']),
                    review_text: reviewText,
                    star_rating: starRating,
                    review_date_text: textOf(['.rsqaWe', 'span.rsqaWe']),
                    owner_response_text: textOf(['.CDe7pd .wiI7pd', '.CDe7pd']),
                    owner_response_date: textOf(['.CDe7pd .rsqaWe']),
                    reviewer_profile_url: attrOf('a[href*=\"/contrib/\"]', 'href'),
                    likes_count: likesCount,
                };
            }"""
        )

    def _record_identity(self, record: ReviewRecord) -> str:
        review_id = (record.raw_review_metadata or {}).get("review_id")
        if review_id:
            return f"{record.place_id}::review_id::{review_id}"
        return f"{record.place_id}::review_key::{record.review_unique_key}"

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
