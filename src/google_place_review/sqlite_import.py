from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class SQLiteImportResult:
    database_path: Path
    jsonl_path: Path
    place_id: str
    scrape_run_id: str
    imported_reviews: int


class SQLiteReviewImporter:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    def import_jsonl(
        self,
        *,
        jsonl_path: Path,
        source_type: str = "playwright_manual",
        debug_json_path: Path | None = None,
        debug_png_path: Path | None = None,
    ) -> SQLiteImportResult:
        rows = self._load_rows(jsonl_path)
        if not rows:
            raise ValueError(f"JSONL 沒有可匯入資料: {jsonl_path}")

        first = rows[0]
        place_id = str(first["place_id"])
        place_name = first.get("place_name") or place_id
        source_url = first.get("source_url") or ""
        scrape_run_id = str(first["scrape_run_id"])
        google_place_token = ((first.get("raw_review_metadata") or {}).get("google_place_token")) or None
        now_iso = datetime.now(UTC).isoformat()
        scraped_values = [self._safe_parse_iso(row.get("scraped_at")) for row in rows if row.get("scraped_at")]
        scraped_values = [value for value in scraped_values if value is not None]
        scraped_started_at = min(scraped_values).isoformat() if scraped_values else None
        scraped_finished_at = max(scraped_values).isoformat() if scraped_values else None

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path)
        try:
            connection.execute("PRAGMA journal_mode=WAL;")
            connection.execute("PRAGMA foreign_keys=ON;")
            self._ensure_schema(connection)
            self._upsert_place(
                connection,
                place_id=place_id,
                place_name=place_name,
                source_url=source_url,
                google_place_token=google_place_token,
                now_iso=now_iso,
            )
            self._upsert_scrape_run(
                connection,
                scrape_run_id=scrape_run_id,
                place_id=place_id,
                source_type=source_type,
                source_url=source_url,
                raw_jsonl_path=str(jsonl_path),
                debug_json_path=str(debug_json_path) if debug_json_path else None,
                debug_png_path=str(debug_png_path) if debug_png_path else None,
                records_in_run=len(rows),
                scraped_started_at=scraped_started_at,
                scraped_finished_at=scraped_finished_at,
            )
            for row in rows:
                self._upsert_review(connection, row)
            connection.commit()
        finally:
            connection.close()

        return SQLiteImportResult(
            database_path=self.database_path,
            jsonl_path=jsonl_path,
            place_id=place_id,
            scrape_run_id=scrape_run_id,
            imported_reviews=len(rows),
        )

    def _ensure_schema(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS places (
                place_id TEXT PRIMARY KEY,
                place_name TEXT NOT NULL,
                source_url TEXT,
                google_place_token TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scrape_runs (
                scrape_run_id TEXT PRIMARY KEY,
                place_id TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_url TEXT,
                raw_jsonl_path TEXT NOT NULL,
                debug_json_path TEXT,
                debug_png_path TEXT,
                records_in_run INTEGER NOT NULL,
                scraped_started_at TEXT,
                scraped_finished_at TEXT,
                FOREIGN KEY(place_id) REFERENCES places(place_id)
            );

            CREATE TABLE IF NOT EXISTS reviews (
                review_unique_key TEXT PRIMARY KEY,
                place_id TEXT NOT NULL,
                scrape_run_id TEXT NOT NULL,
                source_url TEXT NOT NULL,
                review_id TEXT,
                reviewer_name TEXT,
                reviewer_profile_url TEXT,
                review_text TEXT,
                star_rating REAL,
                review_date_text TEXT,
                review_date_estimated TEXT,
                review_date_precision TEXT,
                likes_count INTEGER,
                owner_response_text TEXT,
                owner_response_date TEXT,
                has_owner_response INTEGER NOT NULL DEFAULT 0,
                scraped_at TEXT NOT NULL,
                low_frequency_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY(place_id) REFERENCES places(place_id),
                FOREIGN KEY(scrape_run_id) REFERENCES scrape_runs(scrape_run_id)
            );

            CREATE INDEX IF NOT EXISTS idx_reviews_place_date
                ON reviews(place_id, review_date_estimated);
            CREATE INDEX IF NOT EXISTS idx_reviews_place_rating
                ON reviews(place_id, star_rating);
            CREATE INDEX IF NOT EXISTS idx_reviews_place_likes
                ON reviews(place_id, likes_count);
            CREATE INDEX IF NOT EXISTS idx_reviews_place_owner_response
                ON reviews(place_id, has_owner_response);
            CREATE INDEX IF NOT EXISTS idx_reviews_scrape_run
                ON reviews(scrape_run_id);
            CREATE INDEX IF NOT EXISTS idx_reviews_review_id
                ON reviews(review_id);
            """
        )

    def _upsert_place(
        self,
        connection: sqlite3.Connection,
        *,
        place_id: str,
        place_name: str,
        source_url: str,
        google_place_token: str | None,
        now_iso: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO places (
                place_id, place_name, source_url, google_place_token, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(place_id) DO UPDATE SET
                place_name = excluded.place_name,
                source_url = excluded.source_url,
                google_place_token = excluded.google_place_token,
                updated_at = excluded.updated_at
            """,
            (place_id, place_name, source_url, google_place_token, now_iso, now_iso),
        )

    def _upsert_scrape_run(
        self,
        connection: sqlite3.Connection,
        *,
        scrape_run_id: str,
        place_id: str,
        source_type: str,
        source_url: str,
        raw_jsonl_path: str,
        debug_json_path: str | None,
        debug_png_path: str | None,
        records_in_run: int,
        scraped_started_at: str | None,
        scraped_finished_at: str | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO scrape_runs (
                scrape_run_id, place_id, source_type, source_url, raw_jsonl_path,
                debug_json_path, debug_png_path, records_in_run, scraped_started_at, scraped_finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(scrape_run_id) DO UPDATE SET
                place_id = excluded.place_id,
                source_type = excluded.source_type,
                source_url = excluded.source_url,
                raw_jsonl_path = excluded.raw_jsonl_path,
                debug_json_path = excluded.debug_json_path,
                debug_png_path = excluded.debug_png_path,
                records_in_run = excluded.records_in_run,
                scraped_started_at = excluded.scraped_started_at,
                scraped_finished_at = excluded.scraped_finished_at
            """,
            (
                scrape_run_id,
                place_id,
                source_type,
                source_url,
                raw_jsonl_path,
                debug_json_path,
                debug_png_path,
                records_in_run,
                scraped_started_at,
                scraped_finished_at,
            ),
        )

    def _upsert_review(self, connection: sqlite3.Connection, row: dict[str, Any]) -> None:
        raw_review_metadata = row.get("raw_review_metadata") or {}
        place_id = row.get("place_id")
        review_unique_key = row.get("review_unique_key")
        review_id = raw_review_metadata.get("review_id")
        scraped_at = row.get("scraped_at")
        estimated_date, precision = self._estimate_relative_date(
            review_date_text=row.get("review_date_text"),
            scraped_at=scraped_at,
        )
        owner_response_text = self._normalize_optional_text(row.get("owner_response_text"))
        owner_response_date = self._normalize_optional_text(row.get("owner_response_date"))
        has_owner_response = 1 if owner_response_text else 0
        low_frequency_payload = {
            "review_language": row.get("review_language"),
            "raw_review_metadata": raw_review_metadata,
        }
        existing_review_unique_key = self._find_existing_review_unique_key(
            connection,
            place_id=place_id,
            review_id=review_id,
            reviewer_name=self._normalize_optional_text(row.get("reviewer_name")),
            star_rating=row.get("star_rating"),
            review_date_text=self._normalize_optional_text(row.get("review_date_text")),
        )
        target_review_unique_key = existing_review_unique_key or review_unique_key

        connection.execute(
            """
            INSERT INTO reviews (
                review_unique_key, place_id, scrape_run_id, source_url, review_id,
                reviewer_name, reviewer_profile_url, review_text, star_rating,
                review_date_text, review_date_estimated, review_date_precision,
                likes_count, owner_response_text, owner_response_date,
                has_owner_response, scraped_at, low_frequency_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(review_unique_key) DO UPDATE SET
                place_id = excluded.place_id,
                scrape_run_id = excluded.scrape_run_id,
                source_url = excluded.source_url,
                review_id = excluded.review_id,
                reviewer_name = excluded.reviewer_name,
                reviewer_profile_url = excluded.reviewer_profile_url,
                review_text = excluded.review_text,
                star_rating = excluded.star_rating,
                review_date_text = excluded.review_date_text,
                review_date_estimated = excluded.review_date_estimated,
                review_date_precision = excluded.review_date_precision,
                likes_count = excluded.likes_count,
                owner_response_text = excluded.owner_response_text,
                owner_response_date = excluded.owner_response_date,
                has_owner_response = excluded.has_owner_response,
                scraped_at = excluded.scraped_at,
                low_frequency_json = excluded.low_frequency_json
            """,
            (
                target_review_unique_key,
                place_id,
                row.get("scrape_run_id"),
                row.get("source_url"),
                review_id,
                self._normalize_optional_text(row.get("reviewer_name")),
                self._normalize_optional_text(row.get("reviewer_profile_url")),
                self._normalize_optional_text(row.get("review_text")),
                row.get("star_rating"),
                self._normalize_optional_text(row.get("review_date_text")),
                estimated_date,
                precision,
                row.get("likes_count"),
                owner_response_text,
                owner_response_date,
                has_owner_response,
                scraped_at,
                json.dumps(low_frequency_payload, ensure_ascii=False),
            ),
        )

    def _find_existing_review_unique_key(
        self,
        connection: sqlite3.Connection,
        *,
        place_id: str | None,
        review_id: str | None,
        reviewer_name: str | None,
        star_rating: float | None,
        review_date_text: str | None,
    ) -> str | None:
        if place_id and review_id:
            row = connection.execute(
                """
                SELECT review_unique_key
                FROM reviews
                WHERE place_id = ? AND review_id = ?
                LIMIT 1
                """,
                (place_id, review_id),
            ).fetchone()
            if row:
                return str(row[0])

        if place_id and reviewer_name and review_date_text and star_rating is not None:
            row = connection.execute(
                """
                SELECT review_unique_key
                FROM reviews
                WHERE place_id = ?
                  AND reviewer_name = ?
                  AND review_date_text = ?
                  AND star_rating = ?
                LIMIT 1
                """,
                (place_id, reviewer_name, review_date_text, star_rating),
            ).fetchone()
            if row:
                return str(row[0])

        return None

    def _load_rows(self, jsonl_path: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with jsonl_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    def _estimate_relative_date(self, *, review_date_text: str | None, scraped_at: str | None) -> tuple[str | None, str | None]:
        if not review_date_text or not scraped_at:
            return None, "unknown"

        base = self._safe_parse_iso(scraped_at)
        if base is None:
            return None, "unknown"

        text = str(review_date_text).strip()
        if not text:
            return None, "unknown"

        number = self._extract_first_int(text)
        if number is None:
            return None, "unknown"

        if "天前" in text:
            value = base - timedelta(days=number)
            return value.date().isoformat(), "day"
        if "週前" in text:
            value = base - timedelta(days=number * 7)
            return value.date().isoformat(), "day"
        if "個月前" in text or "月前" in text:
            value = self._subtract_months(base, number)
            return value.date().isoformat(), "month"
        if "年前" in text:
            value = self._subtract_years(base, number)
            return value.date().isoformat(), "year"
        if "小時前" in text:
            value = base - timedelta(hours=number)
            return value.date().isoformat(), "day"
        if "分鐘前" in text:
            return base.date().isoformat(), "day"

        return None, "unknown"

    def _subtract_months(self, value: datetime, months: int) -> datetime:
        year = value.year
        month = value.month - months
        while month <= 0:
            month += 12
            year -= 1
        day = min(value.day, self._days_in_month(year, month))
        return value.replace(year=year, month=month, day=day)

    def _subtract_years(self, value: datetime, years: int) -> datetime:
        year = value.year - years
        day = min(value.day, self._days_in_month(year, value.month))
        return value.replace(year=year, day=day)

    def _days_in_month(self, year: int, month: int) -> int:
        if month == 2:
            leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
            return 29 if leap else 28
        if month in {4, 6, 9, 11}:
            return 30
        return 31

    def _extract_first_int(self, text: str) -> int | None:
        digits = []
        for char in text:
            if char.isdigit():
                digits.append(char)
            elif digits:
                break
        if not digits:
            return None
        return int("".join(digits))

    def _safe_parse_iso(self, value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _normalize_optional_text(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
