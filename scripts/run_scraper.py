from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from google_place_review.config import ScraperConfig
from google_place_review.scraper import GooglePlaceReviewScraper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Google Maps reviews scraper prototype")
    parser.add_argument("--url", required=True, help="Google Maps ????")
    parser.add_argument("--place-id", default=None, help="???? place_id")
    parser.add_argument("--scroll-strategy", choices=["baseline", "scroll_into_view", "action_chains", "hybrid"], default="baseline", help="reviews ??????")
    parser.add_argument("--chromedriver-path", default=None, help="???? chromedriver ?????")
    parser.add_argument("--max-scroll-rounds", type=int, default=60, help="?? scroll ??")
    parser.add_argument("--max-idle-rounds", type=int, default=5, help="??????????")
    parser.add_argument("--max-reviews", type=int, default=None, help="????????")
    parser.add_argument("--debug-save-html", action="store_true", help="??????? HTML")
    parser.add_argument("--panel-interaction-mode", choices=["pagedown", "wheel", "js_scroll"], default="pagedown", help="reviews panel ??????")
    parser.add_argument("--no-panel-focus-before-scroll", action="store_true", help="????????? panel focus")
    parser.add_argument("--no-headless", action="store_true", help="?? headless ??")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = ScraperConfig(
        target_url=args.url,
        headless=not args.no_headless,
        place_id_override=args.place_id,
        scroll_strategy=args.scroll_strategy,
        chromedriver_path=Path(args.chromedriver_path).expanduser() if args.chromedriver_path else None,
        max_scroll_rounds=args.max_scroll_rounds,
        max_idle_rounds=args.max_idle_rounds,
        max_reviews=args.max_reviews,
        debug_save_html=args.debug_save_html,
        panel_interaction_mode=args.panel_interaction_mode,
        panel_focus_before_scroll=not args.no_panel_focus_before_scroll,
    )
    scraper = GooglePlaceReviewScraper(config=config)
    result = scraper.run()

    print(f"place_id: {result.place_id}")
    print(f"place_name: {result.place_name}")
    print(f"run_id: {result.scrape_run_id}")
    print(f"scroll_strategy: {args.scroll_strategy}")
    print(f"panel_interaction_mode: {args.panel_interaction_mode}")
    print(f"total_reviews: {result.total_reviews}")
    print(f"new_reviews_added: {result.new_reviews_added}")
    print(f"latest_output: {result.latest_output_path}")
    print(f"run_output: {result.run_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
