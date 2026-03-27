from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


def load_reviews_from_sqlite(db_path: Path, place_ids: list[str] | None = None) -> pd.DataFrame:
    query = """
    SELECT
        r.place_id,
        p.place_name,
        r.review_unique_key,
        r.review_id,
        r.source_url,
        r.reviewer_name,
        r.reviewer_profile_url,
        r.review_text,
        r.star_rating,
        r.review_date_text,
        r.review_date_estimated,
        r.review_date_precision,
        r.likes_count,
        r.owner_response_text,
        r.owner_response_date,
        r.has_owner_response,
        r.scraped_at,
        r.low_frequency_json
    FROM reviews r
    JOIN places p ON p.place_id = r.place_id
    {where_clause}
    ORDER BY r.place_id ASC, r.scraped_at DESC
    """
    params: list[str] = []
    where_clause = ""
    if place_ids:
        placeholders = ", ".join(["?"] * len(place_ids))
        where_clause = f"WHERE r.place_id IN ({placeholders})"
        params.extend(place_ids)
    final_query = query.format(where_clause=where_clause)
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(final_query, conn, params=params)
