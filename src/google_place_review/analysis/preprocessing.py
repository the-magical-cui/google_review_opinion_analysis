from __future__ import annotations

import json
import re
import unicodedata

import pandas as pd


SPEND_PATTERN = re.compile(
    r"(?:平均每人消費金額|每人消費金額)\s*[:：]?\s*\$?\s*(\d+)(?:\s*[-~～至]\s*\$?\s*(\d+))?"
)
GOOGLE_ASPECT_RATING_PATTERN = re.compile(r"(餐點|服務|氣氛)\s*[:：]\s*([1-5](?:\.\d+)?)")


def normalize_text(text: str | None) -> str:
    if text is None:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    normalized = normalized.replace("\u3000", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def preprocess_reviews(df: pd.DataFrame) -> pd.DataFrame:
    reviews = df.copy()
    reviews["review_text"] = reviews["review_text"].apply(normalize_text)
    reviews["reviewer_name"] = reviews["reviewer_name"].fillna("匿名使用者")
    reviews["place_name"] = reviews["place_name"].fillna(reviews["place_id"])
    reviews["has_text"] = reviews["review_text"].str.len().fillna(0) > 0
    reviews["text_length"] = reviews["review_text"].str.len().fillna(0).astype(int)
    reviews["review_date_estimated_dt"] = pd.to_datetime(reviews["review_date_estimated"], errors="coerce")
    reviews["year"] = reviews["review_date_estimated_dt"].dt.year.astype("Int64")
    reviews["year_month"] = reviews["review_date_estimated_dt"].dt.strftime("%Y-%m")
    reviews.loc[reviews["review_date_estimated_dt"].isna(), "year_month"] = pd.NA
    reviews["likes_count"] = pd.to_numeric(reviews["likes_count"], errors="coerce").fillna(0).astype(int)
    reviews["star_rating"] = pd.to_numeric(reviews["star_rating"], errors="coerce")
    reviews["owner_response_text"] = reviews["owner_response_text"].apply(normalize_text)
    reviews["owner_response_date"] = reviews["owner_response_date"].fillna("")

    spend_amounts = reviews["review_text"].apply(extract_spend_amounts)
    reviews["spend_amount_min"] = spend_amounts.apply(lambda value: value[0])
    reviews["spend_amount_max"] = spend_amounts.apply(lambda value: value[1])
    reviews["spend_amount_mid"] = spend_amounts.apply(lambda value: value[2])
    reviews["has_spend_amount"] = reviews["spend_amount_mid"].notna()

    google_ratings = reviews["review_text"].apply(extract_google_aspect_ratings)
    reviews["google_food_rating"] = google_ratings.apply(lambda value: value.get("餐點"))
    reviews["google_service_rating"] = google_ratings.apply(lambda value: value.get("服務"))
    reviews["google_atmosphere_rating"] = google_ratings.apply(lambda value: value.get("氣氛"))
    reviews["has_google_food_rating"] = reviews["google_food_rating"].notna()
    reviews["has_google_service_rating"] = reviews["google_service_rating"].notna()
    reviews["has_google_atmosphere_rating"] = reviews["google_atmosphere_rating"].notna()

    reviews["sentiment_score"] = 0.0
    reviews["sentiment_label"] = "neutral"
    reviews["low_frequency_payload"] = reviews["low_frequency_json"].apply(_safe_json_loads)
    return reviews


def filter_text_reviews(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["has_text"]].copy()


def extract_spend_amounts(text: str | None) -> tuple[float | None, float | None, float | None]:
    normalized = normalize_text(text)
    if not normalized:
        return None, None, None

    match = SPEND_PATTERN.search(normalized)
    if not match:
        return None, None, None

    min_value = float(match.group(1))
    max_group = match.group(2)
    max_value = float(max_group) if max_group else min_value
    mid_value = (min_value + max_value) / 2.0
    return min_value, max_value, mid_value


def extract_google_aspect_ratings(text: str | None) -> dict[str, float]:
    normalized = normalize_text(text)
    if not normalized:
        return {}

    payload: dict[str, float] = {}
    for aspect_name, value in GOOGLE_ASPECT_RATING_PATTERN.findall(normalized):
        try:
            payload[aspect_name] = float(value)
        except ValueError:
            continue
    return payload


def _safe_json_loads(value: str | None) -> dict:
    if not value:
        return {}
    try:
        payload = json.loads(value)
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}
