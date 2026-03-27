from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from google_place_review.analysis.pipeline import run_single_store_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="執行單店 Google Place reviews 分析")
    parser.add_argument("--db-path", default="data/processed/google_place_reviews.db")
    parser.add_argument("--place-id", required=True)
    parser.add_argument("--output-root", default="data/processed/analysis")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_single_store_pipeline(
        db_path=Path(args.db_path),
        place_id=args.place_id,
        output_root=Path(args.output_root),
    )
    for key, value in result.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
