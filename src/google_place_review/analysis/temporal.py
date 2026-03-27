from __future__ import annotations

import pandas as pd


def aggregate_temporal_metrics(
    df: pd.DataFrame,
    *,
    group_cols: list[str] | None = None,
    period_col: str = "year_month",
) -> pd.DataFrame:
    grouping = list(group_cols or ["place_id"])
    grouping.append(period_col)
    metrics = (
        df.dropna(subset=[period_col])
        .groupby(grouping)
        .agg(
            review_count=("review_unique_key", "count"),
            avg_star_rating=("star_rating", "mean"),
            avg_sentiment_score=("sentiment_score", "mean"),
            avg_spend_amount=("spend_amount_mid", "mean"),
            spend_sample_count=("has_spend_amount", "sum"),
            positive_ratio=("sentiment_label", lambda series: (series == "positive").mean()),
            neutral_ratio=("sentiment_label", lambda series: (series == "neutral").mean()),
            negative_ratio=("sentiment_label", lambda series: (series == "negative").mean()),
        )
        .reset_index()
    )
    return metrics
