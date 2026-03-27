from __future__ import annotations

import re

import pandas as pd

from .dictionaries import ASPECT_KEYWORDS
from .sentiment import label_sentiment
from .sentiment import score_text_sentiment


CLAUSE_SPLIT_PATTERN = re.compile(r"[，。；！？!\?\n]+")
CONNECTOR_PATTERN = re.compile(r"(?:但|可是|不過|然而|結果|加上)")
FOOD_CONTEXT_TOKENS = ("餐", "餐點", "菜", "飲料", "咖啡", "主餐", "正餐")


def run_aspect_analysis(df: pd.DataFrame, aspect_keywords: dict[str, tuple[str, ...]] | None = None) -> pd.DataFrame:
    aspect_map = aspect_keywords or ASPECT_KEYWORDS
    rows: list[dict] = []
    for record in df.itertuples(index=False):
        text = record.review_text or ""
        clauses = _split_into_clauses(text)
        for aspect_name, keywords in aspect_map.items():
            matched_clauses = [clause for clause in clauses if _clause_matches_aspect(aspect_name, clause, keywords)]
            if not matched_clauses:
                continue
            clause_scores = [score_text_sentiment(clause) for clause in matched_clauses]
            score = float(sum(clause_scores) / len(clause_scores))
            rows.append(
                {
                    "place_id": record.place_id,
                    "place_name": record.place_name,
                    "review_unique_key": record.review_unique_key,
                    "reviewer_name": getattr(record, "reviewer_name", None),
                    "star_rating": getattr(record, "star_rating", None),
                    "review_date_text": getattr(record, "review_date_text", None),
                    "review_date_estimated": getattr(record, "review_date_estimated", None),
                    "likes_count": getattr(record, "likes_count", None),
                    "review_text": getattr(record, "review_text", None),
                    "owner_response_text": getattr(record, "owner_response_text", None),
                    "aspect_name": aspect_name,
                    "mentioned": 1,
                    "mention_count_in_review": len(matched_clauses),
                    "aspect_sentiment_score": score,
                    "aspect_sentiment_label": label_sentiment(score),
                }
            )
    if not rows:
        return pd.DataFrame(
            columns=[
                "place_id",
                "place_name",
                "review_unique_key",
                "reviewer_name",
                "star_rating",
                "review_date_text",
                "review_date_estimated",
                "likes_count",
                "review_text",
                "owner_response_text",
                "aspect_name",
                "mentioned",
                "mention_count_in_review",
                "aspect_sentiment_score",
                "aspect_sentiment_label",
            ]
        )
    return pd.DataFrame(rows)


def summarize_aspect_sentiment(aspect_df: pd.DataFrame) -> pd.DataFrame:
    if aspect_df.empty:
        return pd.DataFrame(
            columns=[
                "place_id",
                "aspect_name",
                "mention_count",
                "avg_aspect_sentiment_score",
                "positive_mentions",
                "neutral_mentions",
                "negative_mentions",
                "positive_ratio",
                "neutral_ratio",
                "negative_ratio",
            ]
        )

    summary = (
        aspect_df.groupby(["place_id", "aspect_name"])
        .agg(
            mention_count=("mentioned", "sum"),
            avg_aspect_sentiment_score=("aspect_sentiment_score", "mean"),
            positive_mentions=("aspect_sentiment_label", lambda series: (series == "positive").sum()),
            neutral_mentions=("aspect_sentiment_label", lambda series: (series == "neutral").sum()),
            negative_mentions=("aspect_sentiment_label", lambda series: (series == "negative").sum()),
        )
        .reset_index()
    )
    summary["positive_ratio"] = summary["positive_mentions"] / summary["mention_count"].clip(lower=1)
    summary["neutral_ratio"] = summary["neutral_mentions"] / summary["mention_count"].clip(lower=1)
    summary["negative_ratio"] = summary["negative_mentions"] / summary["mention_count"].clip(lower=1)
    return summary


def _split_into_clauses(text: str) -> list[str]:
    normalized = text.strip()
    if not normalized:
        return []

    parts: list[str] = []
    for segment in CLAUSE_SPLIT_PATTERN.split(normalized):
        cleaned = segment.strip()
        if not cleaned:
            continue
        connector_parts = [piece.strip() for piece in CONNECTOR_PATTERN.split(cleaned) if piece.strip()]
        parts.extend(connector_parts or [cleaned])
    return parts


def _clause_matches_aspect(aspect_name: str, clause: str, keywords: tuple[str, ...]) -> bool:
    if aspect_name != "出餐速度":
        return any(keyword in clause for keyword in keywords)

    if not any(keyword in clause for keyword in keywords):
        return False

    if not any(food_token in clause for food_token in FOOD_CONTEXT_TOKENS):
        return False

    if not any(trigger in clause for trigger in ("等", "上菜", "上餐", "出餐", "還沒上", "等待", "久候")):
        return False

    return True
