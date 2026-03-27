from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


def main() -> int:
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/processed/google_place_reviews.db")
    if not db_path.exists():
        print(f"找不到資料庫：{db_path}")
        return 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        place_rows = conn.execute(
            """
            SELECT
                p.place_id,
                p.place_name,
                (
                    SELECT COUNT(*)
                    FROM reviews r
                    WHERE r.place_id = p.place_id
                ) AS review_count,
                (
                    SELECT COUNT(*)
                    FROM scrape_runs sr
                    WHERE sr.place_id = p.place_id
                ) AS scrape_run_count,
                (
                    SELECT sr2.scrape_run_id
                    FROM scrape_runs sr2
                    WHERE sr2.place_id = p.place_id
                    ORDER BY COALESCE(sr2.scraped_finished_at, sr2.scraped_started_at, sr2.scrape_run_id) DESC
                    LIMIT 1
                ) AS latest_scrape_run_id
            FROM places p
            ORDER BY p.place_name COLLATE NOCASE ASC
            """
        ).fetchall()
        print(f"database_path={db_path}")
        print("places:")
        for row in place_rows:
            print(
                f"- {row['place_name']} ({row['place_id']}): "
                f"reviews={row['review_count']}, runs={row['scrape_run_count']}, latest_run={row['latest_scrape_run_id']}"
            )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
