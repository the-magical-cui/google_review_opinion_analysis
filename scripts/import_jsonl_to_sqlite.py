from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from google_place_review.sqlite_import import SQLiteReviewImporter


def _default_runs_dir(place_id: str) -> Path:
    return Path("data/playwright") / place_id / "runs"


def _latest_reviews_jsonl(place_id: str) -> Path:
    runs_dir = _default_runs_dir(place_id)
    candidates = sorted(runs_dir.glob("reviews_*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"找不到 {place_id} 的 reviews_*.jsonl：{runs_dir}")
    return candidates[0]


def _infer_debug_path(jsonl_path: Path, suffix: str) -> Path:
    if not jsonl_path.name.startswith("reviews_"):
        raise ValueError(f"無法從檔名推回 debug 路徑：{jsonl_path.name}")
    run_id = jsonl_path.stem.replace("reviews_", "", 1)
    return jsonl_path.with_name(f"debug_{run_id}.{suffix}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="將 Google Place Review JSONL 匯入 SQLite。")
    parser.add_argument(
        "--jsonl-path",
        default=None,
        help="要匯入的 JSONL 路徑；若未提供，會改用 --place-id 的最新 run。",
    )
    parser.add_argument(
        "--place-id",
        default="rebirth",
        help="若未提供 --jsonl-path，會從 data/playwright/<place_id>/runs/ 自動挑最新 JSONL。",
    )
    parser.add_argument(
        "--db-path",
        default="data/processed/google_place_reviews.db",
        help="SQLite 資料庫路徑。",
    )
    parser.add_argument(
        "--source-type",
        default="playwright_manual",
        help="匯入來源類型標記。",
    )
    parser.add_argument(
        "--debug-json-path",
        default=None,
        help="對應 debug JSON 路徑；若未提供，會依 JSONL 檔名自動推回。",
    )
    parser.add_argument(
        "--debug-png-path",
        default=None,
        help="對應 debug PNG 路徑；若未提供，會依 JSONL 檔名自動推回。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    jsonl_path = Path(args.jsonl_path) if args.jsonl_path else _latest_reviews_jsonl(args.place_id)
    debug_json_path = Path(args.debug_json_path) if args.debug_json_path else _infer_debug_path(jsonl_path, "json")
    debug_png_path = Path(args.debug_png_path) if args.debug_png_path else _infer_debug_path(jsonl_path, "png")
    importer = SQLiteReviewImporter(Path(args.db_path))
    result = importer.import_jsonl(
        jsonl_path=jsonl_path,
        source_type=args.source_type,
        debug_json_path=debug_json_path if debug_json_path.exists() else None,
        debug_png_path=debug_png_path if debug_png_path.exists() else None,
    )
    print(f"database_path={result.database_path}")
    print(f"jsonl_path={result.jsonl_path}")
    print(f"place_id={result.place_id}")
    print(f"scrape_run_id={result.scrape_run_id}")
    print(f"imported_reviews={result.imported_reviews}")


if __name__ == "__main__":
    main()
