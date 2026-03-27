from __future__ import annotations

from collections import Counter

import jieba
import pandas as pd

from .dictionaries import INTENSIFIERS, NEGATIONS, NEGATIVE_WORDS, POSITIVE_WORDS
from .preprocessing import normalize_text


def score_text_sentiment(text: str | None) -> float:
    normalized = normalize_text(text)
    if not normalized:
        return 0.0
    tokens = [token.strip() for token in jieba.lcut(normalized) if token.strip()]
    if not tokens:
        return 0.0

    score = 0.0
    for index, token in enumerate(tokens):
        base = 0.0
        if token in POSITIVE_WORDS:
            base = 1.0
        elif token in NEGATIVE_WORDS:
            base = -1.0
        if base == 0.0:
            continue

        prev_token = tokens[index - 1] if index > 0 else ""
        if prev_token in INTENSIFIERS:
            base *= 1.5
        if prev_token in NEGATIONS:
            base *= -1
        score += base

    if score > 0.5:
        return min(score, 5.0)
    if score < -0.5:
        return max(score, -5.0)
    return score


def label_sentiment(score: float) -> str:
    if score > 0.5:
        return "positive"
    if score < -0.5:
        return "negative"
    return "neutral"


def run_sentiment_analysis(df: pd.DataFrame) -> pd.DataFrame:
    reviews = df.copy()
    reviews["sentiment_score"] = reviews["review_text"].apply(score_text_sentiment).astype(float)
    reviews["sentiment_label"] = reviews["sentiment_score"].apply(label_sentiment)
    return reviews


def summarize_sentiment(df: pd.DataFrame, group_col: str = "place_id") -> pd.DataFrame:
    grouped = (
        df.groupby(group_col)
        .agg(
            review_count=("review_unique_key", "count"),
            avg_sentiment_score=("sentiment_score", "mean"),
            avg_star_rating=("star_rating", "mean"),
            avg_spend_amount=("spend_amount_mid", "mean"),
            spend_sample_count=("has_spend_amount", "sum"),
            avg_google_food_rating=("google_food_rating", "mean"),
            google_food_rating_count=("has_google_food_rating", "sum"),
            avg_google_service_rating=("google_service_rating", "mean"),
            google_service_rating_count=("has_google_service_rating", "sum"),
            avg_google_atmosphere_rating=("google_atmosphere_rating", "mean"),
            google_atmosphere_rating_count=("has_google_atmosphere_rating", "sum"),
        )
        .reset_index()
    )
    distribution = (
        df.groupby([group_col, "sentiment_label"])
        .size()
        .reset_index(name="count")
        .pivot(index=group_col, columns="sentiment_label", values="count")
        .fillna(0)
        .reset_index()
    )
    summary = grouped.merge(distribution, on=group_col, how="left").fillna(0)
    for label in ("positive", "neutral", "negative"):
        if label not in summary:
            summary[label] = 0
        summary[f"{label}_ratio"] = summary[label] / summary["review_count"].clip(lower=1)
    return summary


def sentiment_label_counts(df: pd.DataFrame) -> Counter:
    return Counter(df["sentiment_label"].tolist())
