from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from google_place_review_playwright.probe import PlaywrightProbeConfig
from google_place_review_playwright.scrape_once import PlaywrightReviewScrapeOnce


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Google Maps Playwright scrape once")
    parser.add_argument(
        "--entry-mode",
        choices=["attach_existing_reviews_page"],
        default="attach_existing_reviews_page",
        help="入口模式",
    )
    parser.add_argument("--max-rounds", type=int, default=20, help="最大滾動輪數")
    parser.add_argument("--debug-save-html", action="store_true", help="額外保存 HTML")
    parser.add_argument("--no-headless", action="store_true", help="使用可見瀏覽器")
    parser.add_argument(
        "--profile-dir",
        default="data/playwright/profile/manual_google_maps",
        help="Playwright persistent profile 路徑",
    )
    parser.add_argument(
        "--manual-ready-timeout",
        type=int,
        default=180,
        help="等待手動準備評論頁的秒數",
    )
    parser.add_argument("--place-id", default="rebirth", help="輸出的 place_id")
    parser.add_argument("--place-name", default="Rebirth", help="輸出的 place_name")
    parser.add_argument("--expect-reviews-tab", action="store_true", help="若你已切到評論頁可開啟此旗標")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = PlaywrightProbeConfig(
        entry_mode=args.entry_mode,
        headless=not args.no_headless,
        max_rounds=args.max_rounds,
        debug_save_html=args.debug_save_html,
        profile_dir=Path(args.profile_dir),
        manual_ready_timeout=args.manual_ready_timeout,
        expect_reviews_tab=args.expect_reviews_tab,
        place_name_hint=args.place_name,
        place_slug_override=args.place_id,
    )
    result = PlaywrightReviewScrapeOnce(
        config,
        place_id=args.place_id,
        place_name=args.place_name,
    ).run()
    print(f"place_id: {result.place_id}")
    print(f"place_name: {result.place_name}")
    print(f"scrape_run_id: {result.scrape_run_id}")
    print(f"review_count: {result.review_count}")
    print(f"output_jsonl: {result.output_jsonl_path}")
    print(f"output_debug_json: {result.output_debug_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
