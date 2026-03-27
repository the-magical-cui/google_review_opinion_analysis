from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from google_place_review_playwright import PlaywrightProbe, PlaywrightProbeConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Google Maps Playwright probe")
    parser.add_argument("--url", help="Google Maps place URL or review-intent URL")
    parser.add_argument(
        "--entry-mode",
        choices=["direct_url", "search_then_click_place", "attach_existing_reviews_page"],
        default="direct_url",
        help="入口模式",
    )
    parser.add_argument("--search-query", help="搜尋店名關鍵字")
    parser.add_argument("--place-name-hint", help="店名提示，用來挑選搜尋結果")
    parser.add_argument("--max-rounds", type=int, default=6, help="每種 lazy-load 策略的最大步數")
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
        default=120,
        help="半自動模式下等待手動準備頁面的秒數",
    )
    parser.add_argument(
        "--expect-reviews-tab",
        action="store_true",
        help="若你已手動切到評論 tab，可用此旗標加強驗證",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.entry_mode == "direct_url" and not args.url:
        raise SystemExit("direct_url 模式必須提供 --url")
    if args.entry_mode == "search_then_click_place" and not (args.search_query or args.place_name_hint):
        raise SystemExit("search_then_click_place 模式必須提供 --search-query 或 --place-name-hint")

    config = PlaywrightProbeConfig(
        target_url=args.url,
        entry_mode=args.entry_mode,
        search_query=args.search_query,
        place_name_hint=args.place_name_hint,
        headless=not args.no_headless,
        max_rounds=args.max_rounds,
        debug_save_html=args.debug_save_html,
        profile_dir=Path(args.profile_dir),
        manual_ready_timeout=args.manual_ready_timeout,
        expect_reviews_tab=args.expect_reviews_tab,
    )
    result = PlaywrightProbe(config).run()
    print(f"place_slug: {result.place_slug}")
    print(f"run_id: {result.run_id}")
    print(f"pane_found: {result.pane_found}")
    print(f"entry_verdict: {result.entry_verdict}")
    print(f"review_card_count: {result.review_card_count}")
    print(f"output_json: {result.output_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
