from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(slots=True)
class PlaceOption:
    place_id: str
    place_name: str
    source_url: str | None

    @property
    def label(self) -> str:
        return f"{self.place_name} ({self.place_id})"


@dataclass(slots=True)
class PlaceSummary:
    place_id: str
    place_name: str
    source_url: str | None
    total_reviews: int
    latest_scrape_run_id: str | None
    scrape_run_count: int


@dataclass(slots=True)
class ReviewFilters:
    likes_sort_desc: bool = False
    likes_min: int | None = None
    only_owner_response: bool = False
    time_sort_desc: bool = True
    star_ratings: tuple[int, ...] = ()


@dataclass(slots=True)
class ReviewRow:
    review_unique_key: str
    reviewer_name: str | None
    reviewer_profile_url: str | None
    review_text: str | None
    star_rating: float | None
    review_date_text: str | None
    review_date_estimated: str | None
    likes_count: int | None
    owner_response_text: str | None
    owner_response_date: str | None
    has_owner_response: bool
    scraped_at: str


@dataclass(slots=True)
class ReviewPage:
    rows: list[ReviewRow]
    total_count: int
    page: int
    page_size: int
    total_pages: int


class ReviewQueryService:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.analysis_root = db_path.parent / "analysis"

    def get_available_places(self) -> list[PlaceOption]:
        query = """
        SELECT place_id, place_name, source_url
        FROM places
        ORDER BY place_name COLLATE NOCASE ASC, place_id ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query).fetchall()
        return [
            PlaceOption(
                place_id=row["place_id"],
                place_name=row["place_name"],
                source_url=row["source_url"],
            )
            for row in rows
        ]

    def search_places(self, keyword: str) -> list[PlaceOption]:
        keyword = keyword.strip()
        if not keyword:
            return self.get_available_places()
        pattern = f"%{keyword}%"
        query = """
        SELECT place_id, place_name, source_url
        FROM places
        WHERE place_name LIKE ? OR place_id LIKE ?
        ORDER BY place_name COLLATE NOCASE ASC, place_id ASC
        """
        with self._connect() as conn:
            rows = conn.execute(query, (pattern, pattern)).fetchall()
        return [
            PlaceOption(
                place_id=row["place_id"],
                place_name=row["place_name"],
                source_url=row["source_url"],
            )
            for row in rows
        ]

    def get_place_summary(self, place_id: str) -> PlaceSummary | None:
        query = """
        SELECT
            p.place_id,
            p.place_name,
            p.source_url,
            COUNT(r.review_unique_key) AS total_reviews,
            (
                SELECT sr.scrape_run_id
                FROM scrape_runs sr
                WHERE sr.place_id = p.place_id
                ORDER BY COALESCE(sr.scraped_finished_at, sr.scraped_started_at, sr.scrape_run_id) DESC
                LIMIT 1
            ) AS latest_scrape_run_id,
            (
                SELECT COUNT(*)
                FROM scrape_runs sr2
                WHERE sr2.place_id = p.place_id
            ) AS scrape_run_count
        FROM places p
        LEFT JOIN reviews r ON r.place_id = p.place_id
        WHERE p.place_id = ?
        GROUP BY p.place_id, p.place_name, p.source_url
        """
        with self._connect() as conn:
            row = conn.execute(query, (place_id,)).fetchone()
        if row is None:
            return None
        return PlaceSummary(
            place_id=row["place_id"],
            place_name=row["place_name"],
            source_url=row["source_url"],
            total_reviews=row["total_reviews"] or 0,
            latest_scrape_run_id=row["latest_scrape_run_id"],
            scrape_run_count=row["scrape_run_count"] or 0,
        )

    def get_monthly_review_counts(self, place_id: str, months: int = 12) -> pd.DataFrame:
        query = """
        WITH bounds AS (
            SELECT date('now', 'start of month', ?) AS start_month
        )
        SELECT substr(review_date_estimated, 1, 7) AS period, COUNT(*) AS review_count
        FROM reviews, bounds
        WHERE place_id = ?
          AND review_date_estimated IS NOT NULL
          AND date(review_date_estimated) >= bounds.start_month
        GROUP BY substr(review_date_estimated, 1, 7)
        ORDER BY period ASC
        """
        month_offset = f"-{max(0, months - 1)} months"
        return self._frame(query, (month_offset, place_id), ["period", "review_count"])

    def get_monthly_avg_stars(self, place_id: str, months: int = 12) -> pd.DataFrame:
        query = """
        WITH bounds AS (
            SELECT date('now', 'start of month', ?) AS start_month
        )
        SELECT substr(review_date_estimated, 1, 7) AS period, AVG(star_rating) AS avg_star
        FROM reviews, bounds
        WHERE place_id = ?
          AND review_date_estimated IS NOT NULL
          AND star_rating IS NOT NULL
          AND date(review_date_estimated) >= bounds.start_month
        GROUP BY substr(review_date_estimated, 1, 7)
        ORDER BY period ASC
        """
        month_offset = f"-{max(0, months - 1)} months"
        return self._frame(query, (month_offset, place_id), ["period", "avg_star"])

    def get_previous_month_star_distribution(self, place_id: str) -> pd.DataFrame:
        query = """
        WITH month_bounds AS (
            SELECT
                date('now', 'start of month', '-1 month') AS start_date,
                date('now', 'start of month') AS end_date
        )
        SELECT CAST(star_rating AS INTEGER) AS star_rating, COUNT(*) AS review_count
        FROM reviews, month_bounds
        WHERE place_id = ?
          AND review_date_estimated IS NOT NULL
          AND star_rating IS NOT NULL
          AND date(review_date_estimated) >= month_bounds.start_date
          AND date(review_date_estimated) < month_bounds.end_date
        GROUP BY CAST(star_rating AS INTEGER)
        ORDER BY star_rating ASC
        """
        return self._frame(query, (place_id,), ["star_rating", "review_count"])

    def get_yearly_review_counts(self, place_id: str) -> pd.DataFrame:
        query = """
        SELECT substr(review_date_estimated, 1, 4) AS period, COUNT(*) AS review_count
        FROM reviews
        WHERE place_id = ?
          AND review_date_estimated IS NOT NULL
        GROUP BY substr(review_date_estimated, 1, 4)
        ORDER BY period ASC
        """
        return self._frame(query, (place_id,), ["period", "review_count"])

    def get_yearly_avg_stars(self, place_id: str) -> pd.DataFrame:
        query = """
        SELECT substr(review_date_estimated, 1, 4) AS period, AVG(star_rating) AS avg_star
        FROM reviews
        WHERE place_id = ?
          AND review_date_estimated IS NOT NULL
          AND star_rating IS NOT NULL
        GROUP BY substr(review_date_estimated, 1, 4)
        ORDER BY period ASC
        """
        return self._frame(query, (place_id,), ["period", "avg_star"])

    def get_yearly_star_distribution(self, place_id: str) -> pd.DataFrame:
        query = """
        SELECT
            substr(review_date_estimated, 1, 4) AS period,
            CAST(star_rating AS INTEGER) AS star_rating,
            COUNT(*) AS review_count
        FROM reviews
        WHERE place_id = ?
          AND review_date_estimated IS NOT NULL
          AND star_rating IS NOT NULL
        GROUP BY substr(review_date_estimated, 1, 4), CAST(star_rating AS INTEGER)
        ORDER BY period ASC, star_rating ASC
        """
        return self._frame(query, (place_id,), ["period", "star_rating", "review_count"])

    def get_reviews_page(self, place_id: str, filters: ReviewFilters, page: int, page_size: int = 20) -> ReviewPage:
        conditions = ["place_id = ?"]
        params: list[Any] = [place_id]

        if filters.likes_min is not None:
            conditions.append("COALESCE(likes_count, 0) >= ?")
            params.append(filters.likes_min)
        if filters.only_owner_response:
            conditions.append("has_owner_response = 1")
        if filters.star_ratings:
            placeholders = ", ".join(["?"] * len(filters.star_ratings))
            conditions.append(f"CAST(star_rating AS INTEGER) IN ({placeholders})")
            params.extend(filters.star_ratings)

        where_clause = " AND ".join(conditions)
        order_by = self._build_order_by(filters)

        count_query = f"SELECT COUNT(*) AS total_count FROM reviews WHERE {where_clause}"
        with self._connect() as conn:
            total_count = conn.execute(count_query, params).fetchone()["total_count"]

        total_pages = max(1, (total_count + page_size - 1) // page_size)
        safe_page = max(1, min(page, total_pages))
        offset = (safe_page - 1) * page_size

        data_query = f"""
        SELECT
            review_unique_key,
            reviewer_name,
            reviewer_profile_url,
            review_text,
            star_rating,
            review_date_text,
            review_date_estimated,
            likes_count,
            owner_response_text,
            owner_response_date,
            has_owner_response,
            scraped_at
        FROM reviews
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
        """
        query_params = [*params, page_size, offset]
        with self._connect() as conn:
            rows = conn.execute(data_query, query_params).fetchall()

        records = [
            ReviewRow(
                review_unique_key=row["review_unique_key"],
                reviewer_name=row["reviewer_name"],
                reviewer_profile_url=row["reviewer_profile_url"],
                review_text=row["review_text"],
                star_rating=row["star_rating"],
                review_date_text=row["review_date_text"],
                review_date_estimated=row["review_date_estimated"],
                likes_count=row["likes_count"],
                owner_response_text=row["owner_response_text"],
                owner_response_date=row["owner_response_date"],
                has_owner_response=bool(row["has_owner_response"]),
                scraped_at=row["scraped_at"],
            )
            for row in rows
        ]
        return ReviewPage(
            rows=records,
            total_count=total_count,
            page=safe_page,
            page_size=page_size,
            total_pages=total_pages,
        )

    def get_single_store_sentiment_summary(self, place_id: str) -> pd.DataFrame:
        return self._read_csv(
            self.analysis_root / "single_store" / place_id / "sentiment_summary.csv"
        )

    def get_single_store_monthly_metrics(self, place_id: str) -> pd.DataFrame:
        return self._read_csv(
            self.analysis_root / "single_store" / place_id / "monthly_metrics.csv"
        )

    def get_single_store_yearly_metrics(self, place_id: str) -> pd.DataFrame:
        return self._read_csv(
            self.analysis_root / "single_store" / place_id / "yearly_metrics.csv"
        )

    def get_single_store_aspect_summary(self, place_id: str) -> pd.DataFrame:
        return self._read_csv(
            self.analysis_root / "single_store" / place_id / "aspect_summary.csv"
        )

    def get_single_store_aspect_mentions(self, place_id: str, aspect_name: str | None = None) -> pd.DataFrame:
        frame = self._read_csv(
            self.analysis_root / "single_store" / place_id / "aspect_mentions_enriched.csv"
        )
        if frame.empty:
            return frame
        if aspect_name:
            frame = frame[frame["aspect_name"] == aspect_name].copy()
        return frame

    def get_single_store_lexical_terms(self, place_id: str, limit: int = 20) -> pd.DataFrame:
        frame = self._read_csv(
            self.analysis_root / "single_store" / place_id / "lexical_top_terms.csv"
        )
        if frame.empty:
            return frame
        return frame.head(limit)

    def get_single_store_collocations(self, place_id: str, limit: int = 20) -> pd.DataFrame:
        frame = self._read_csv(
            self.analysis_root / "single_store" / place_id / "lexical_collocations.csv"
        )
        if frame.empty:
            return frame
        return frame.sort_values(["rank", "mention_count"], ascending=[True, False]).head(limit)

    def get_single_store_figure_paths(self, place_id: str) -> dict[str, Path]:
        figures_dir = self.analysis_root / "single_store" / place_id / "figures"
        return {
            "monthly_review_count": figures_dir / "monthly_review_count.png",
            "monthly_avg_star": figures_dir / "monthly_avg_star.png",
            "monthly_avg_sentiment": figures_dir / "monthly_avg_sentiment.png",
        }

    def get_cross_store_sentiment_comparison(self) -> pd.DataFrame:
        frame = self._read_csv(self.analysis_root / "cross_store" / "sentiment_comparison.csv")
        if frame.empty:
            return frame
        return self._attach_place_names(frame)

    def get_cross_store_aspect_comparison(self, min_mentions: int = 10) -> pd.DataFrame:
        frame = self._read_csv(self.analysis_root / "cross_store" / "aspect_comparison.csv")
        if frame.empty:
            return frame
        frame = frame[frame["mention_count"] >= min_mentions].copy()
        return self._attach_place_names(frame)

    def get_cross_store_tfidf_terms(self, top_n: int = 10) -> pd.DataFrame:
        frame = self._read_csv(self.analysis_root / "cross_store" / "tfidf_distinctive_terms.csv")
        if frame.empty:
            return frame
        return frame[frame["rank"] <= top_n].copy()

    def get_cross_store_monthly_metrics(self) -> pd.DataFrame:
        frame = self._read_csv(self.analysis_root / "cross_store" / "monthly_metrics.csv")
        if frame.empty:
            return frame
        return self._attach_place_names(frame)

    def get_cross_store_yearly_metrics(self) -> pd.DataFrame:
        frame = self._read_csv(self.analysis_root / "cross_store" / "yearly_metrics.csv")
        if frame.empty:
            return frame
        return self._attach_place_names(frame)

    def get_cross_store_star_distribution(self) -> pd.DataFrame:
        query = """
        SELECT
            p.place_name,
            CAST(r.star_rating AS INTEGER) AS star_rating,
            COUNT(*) AS review_count
        FROM reviews r
        JOIN places p ON p.place_id = r.place_id
        WHERE r.star_rating IS NOT NULL
        GROUP BY p.place_name, CAST(r.star_rating AS INTEGER)
        ORDER BY p.place_name ASC, star_rating ASC
        """
        return self._frame(query, tuple(), ["place_name", "star_rating", "review_count"])

    def get_cross_store_figure_paths(self) -> dict[str, Path]:
        figures_dir = self.analysis_root / "cross_store" / "figures"
        return {
            "cross_store_avg_sentiment": figures_dir / "cross_store_avg_sentiment.png",
        }

    def _build_order_by(self, filters: ReviewFilters) -> str:
        if filters.likes_sort_desc:
            return "COALESCE(likes_count, -1) DESC, COALESCE(review_date_estimated, scraped_at) DESC, scraped_at DESC"
        if filters.time_sort_desc:
            return "COALESCE(review_date_estimated, scraped_at) DESC, scraped_at DESC"
        return "scraped_at DESC"

    def _frame(self, query: str, params: tuple[Any, ...], columns: list[str]) -> pd.DataFrame:
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return pd.DataFrame([tuple(row) for row in rows], columns=columns)

    def _read_csv(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    def _attach_place_names(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty or "place_id" not in frame.columns:
            return frame
        places = pd.DataFrame(
            [
                {"place_id": place.place_id, "place_name": place.place_name}
                for place in self.get_available_places()
            ]
        )
        if places.empty:
            return frame
        if "place_name" in frame.columns:
            merged = places.merge(frame.drop(columns=["place_name"]), on="place_id", how="right")
            return merged
        return places.merge(frame, on="place_id", how="right")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
