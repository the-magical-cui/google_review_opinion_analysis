from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from .aspects import run_aspect_analysis
from .aspects import summarize_aspect_sentiment
from .comparative import run_cross_store_comparison
from .comparative import run_tfidf_comparison
from .loading import load_reviews_from_sqlite
from .lexical import run_collocation_analysis
from .lexical import run_lexical_analysis
from .preprocessing import filter_text_reviews
from .preprocessing import preprocess_reviews
from .regression import build_review_feature_frame
from .regression import compute_aspect_star_relation
from .regression import run_review_level_regression
from .regression import save_regression_figures
from .sentiment import run_sentiment_analysis
from .sentiment import summarize_sentiment
from .temporal import aggregate_temporal_metrics
from .visualization import save_bar_chart
from .visualization import save_line_chart
from .visualization import save_common_aspect_grid
from .visualization import save_multi_line_chart
from .visualization import save_sentiment_distribution_chart


def run_single_store_pipeline(*, db_path: Path, place_id: str, output_root: Path) -> dict[str, Path]:
    output_dir = output_root / "single_store" / place_id
    figures_dir = output_dir / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    raw_df = load_reviews_from_sqlite(db_path, [place_id])
    reviews = preprocess_reviews(raw_df)
    text_reviews = filter_text_reviews(reviews)
    sentiment_df = run_sentiment_analysis(text_reviews)
    monthly_metrics = aggregate_temporal_metrics(sentiment_df, group_cols=["place_id"], period_col="year_month")
    yearly_metrics = aggregate_temporal_metrics(sentiment_df, group_cols=["place_id"], period_col="year")
    aspect_mentions = run_aspect_analysis(sentiment_df)
    aspect_summary = summarize_aspect_sentiment(aspect_mentions)
    lexical_top_terms = run_lexical_analysis(sentiment_df, group_col="place_id", top_n=25)
    lexical_collocations = run_collocation_analysis(sentiment_df, lexical_top_terms, group_col="place_id")
    sentiment_summary = summarize_sentiment(sentiment_df, group_col="place_id")

    sentiment_df.to_csv(output_dir / "sentiment_reviews.csv", index=False, encoding="utf-8-sig")
    monthly_metrics.to_csv(output_dir / "monthly_metrics.csv", index=False, encoding="utf-8-sig")
    yearly_metrics.to_csv(output_dir / "yearly_metrics.csv", index=False, encoding="utf-8-sig")
    aspect_mentions.to_csv(output_dir / "aspect_mentions.csv", index=False, encoding="utf-8-sig")
    aspect_mentions.to_csv(output_dir / "aspect_mentions_enriched.csv", index=False, encoding="utf-8-sig")
    aspect_summary.to_csv(output_dir / "aspect_summary.csv", index=False, encoding="utf-8-sig")
    lexical_top_terms.to_csv(output_dir / "lexical_top_terms.csv", index=False, encoding="utf-8-sig")
    lexical_collocations.to_csv(output_dir / "lexical_collocations.csv", index=False, encoding="utf-8-sig")
    sentiment_summary.to_csv(output_dir / "sentiment_summary.csv", index=False, encoding="utf-8-sig")

    _write_json(output_dir / "sentiment_summary.json", sentiment_summary.to_dict(orient="records"))

    if not monthly_metrics.empty:
        save_bar_chart(
            monthly_metrics,
            x="year_month",
            y="review_count",
            title=f"{place_id} 每月評論數",
            output_path=figures_dir / "monthly_review_count.png",
        )
        save_line_chart(
            monthly_metrics,
            x="year_month",
            y="avg_star_rating",
            title=f"{place_id} 每月平均星數",
            output_path=figures_dir / "monthly_avg_star.png",
        )
        save_line_chart(
            monthly_metrics,
            x="year_month",
            y="avg_sentiment_score",
            title=f"{place_id} 每月平均情緒分數",
            output_path=figures_dir / "monthly_avg_sentiment.png",
        )

    return {
        "output_dir": output_dir,
        "sentiment_summary_csv": output_dir / "sentiment_summary.csv",
        "aspect_summary_csv": output_dir / "aspect_summary.csv",
        "lexical_top_terms_csv": output_dir / "lexical_top_terms.csv",
        "lexical_collocations_csv": output_dir / "lexical_collocations.csv",
    }


def run_cross_store_pipeline(*, db_path: Path, place_ids: list[str], output_root: Path) -> dict[str, Path]:
    output_dir = output_root / "cross_store"
    figures_dir = output_dir / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    raw_df = load_reviews_from_sqlite(db_path, place_ids)
    reviews = preprocess_reviews(raw_df)
    text_reviews = filter_text_reviews(reviews)
    sentiment_df = run_sentiment_analysis(text_reviews)
    aspect_mentions = run_aspect_analysis(sentiment_df)
    comparison = run_cross_store_comparison(sentiment_df, aspect_mentions, min_mentions=5)
    tfidf_terms = run_tfidf_comparison(sentiment_df, top_n=20)
    place_names = sentiment_df[["place_id", "place_name"]].drop_duplicates()
    monthly_metrics = aggregate_temporal_metrics(sentiment_df, group_cols=["place_id"], period_col="year_month")
    yearly_metrics = aggregate_temporal_metrics(sentiment_df, group_cols=["place_id"], period_col="year")
    monthly_metrics = place_names.merge(monthly_metrics, on="place_id", how="right")
    yearly_metrics = place_names.merge(yearly_metrics, on="place_id", how="right")
    review_features = build_review_feature_frame(sentiment_df, aspect_mentions)
    aspect_star_relation = compute_aspect_star_relation(review_features)
    regression_summary, regression_coefficients = run_review_level_regression(review_features)

    comparison["sentiment_comparison"].to_csv(output_dir / "sentiment_comparison.csv", index=False, encoding="utf-8-sig")
    comparison["aspect_comparison"].to_csv(output_dir / "aspect_comparison.csv", index=False, encoding="utf-8-sig")
    tfidf_terms.to_csv(output_dir / "tfidf_distinctive_terms.csv", index=False, encoding="utf-8-sig")
    monthly_metrics.to_csv(output_dir / "monthly_metrics.csv", index=False, encoding="utf-8-sig")
    yearly_metrics.to_csv(output_dir / "yearly_metrics.csv", index=False, encoding="utf-8-sig")
    review_features.to_csv(output_dir / "review_feature_frame.csv", index=False, encoding="utf-8-sig")
    aspect_star_relation.to_csv(output_dir / "aspect_star_relation.csv", index=False, encoding="utf-8-sig")
    regression_summary.to_csv(output_dir / "regression_summary.csv", index=False, encoding="utf-8-sig")
    regression_coefficients.to_csv(output_dir / "regression_coefficients.csv", index=False, encoding="utf-8-sig")

    _write_json(output_dir / "sentiment_comparison.json", comparison["sentiment_comparison"].to_dict(orient="records"))
    _write_json(output_dir / "aspect_comparison.json", comparison["aspect_comparison"].to_dict(orient="records"))
    _write_json(output_dir / "tfidf_distinctive_terms.json", tfidf_terms.to_dict(orient="records"))
    _write_json(output_dir / "regression_summary.json", regression_summary.to_dict(orient="records"))
    _write_json(output_dir / "regression_coefficients.json", regression_coefficients.to_dict(orient="records"))

    if not comparison["sentiment_comparison"].empty:
        save_bar_chart(
            comparison["sentiment_comparison"],
            x="place_id",
            y="avg_sentiment_score",
            title="跨店平均情緒分數比較",
            output_path=figures_dir / "cross_store_avg_sentiment.png",
        )
    if not comparison["sentiment_comparison"].empty:
        save_bar_chart(
            comparison["sentiment_comparison"],
            x="place_name",
            y="review_count",
            title="三家店評論數總覽",
            output_path=figures_dir / "cross_store_review_count.png",
        )
        sentiment_dist_df = comparison["sentiment_comparison"].copy()
        sentiment_dist_df["display_label"] = sentiment_dist_df["place_name"]
        save_sentiment_distribution_chart(
            sentiment_dist_df,
            label_col="display_label",
            positive_col="positive_ratio",
            neutral_col="neutral_ratio",
            negative_col="negative_ratio",
            title="跨店情緒分布比較",
            output_path=figures_dir / "cross_store_sentiment_distribution.png",
        )
    if not monthly_metrics.empty:
        save_multi_line_chart(
            monthly_metrics,
            x="year_month",
            y="review_count",
            series="place_name",
            title="跨店每月評論數比較",
            output_path=figures_dir / "cross_store_monthly_review_count.png",
        )
        save_multi_line_chart(
            monthly_metrics,
            x="year_month",
            y="avg_star_rating",
            series="place_name",
            title="跨店每月平均星數比較",
            output_path=figures_dir / "cross_store_monthly_avg_star.png",
        )
    if not yearly_metrics.empty:
        save_multi_line_chart(
            yearly_metrics,
            x="year",
            y="avg_star_rating",
            series="place_name",
            title="跨店每年平均星數比較",
            output_path=figures_dir / "cross_store_yearly_avg_star.png",
        )
    if not comparison["aspect_comparison"].empty:
        save_common_aspect_grid(
            comparison["aspect_comparison"],
            aspect_col="aspect_name",
            series_col="place_name",
            value_col="avg_aspect_sentiment_score",
            title="共同面向平均情緒比較",
            output_path=figures_dir / "cross_store_common_aspects.png",
        )
    save_regression_figures(
        review_features=review_features,
        aspect_relation_df=aspect_star_relation,
        figures_dir=figures_dir,
    )

    return {
        "output_dir": output_dir,
        "sentiment_comparison_csv": output_dir / "sentiment_comparison.csv",
        "aspect_comparison_csv": output_dir / "aspect_comparison.csv",
        "tfidf_terms_csv": output_dir / "tfidf_distinctive_terms.csv",
        "aspect_star_relation_csv": output_dir / "aspect_star_relation.csv",
        "regression_summary_csv": output_dir / "regression_summary.csv",
        "regression_coefficients_csv": output_dir / "regression_coefficients.csv",
    }


def _write_json(path: Path, payload: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
