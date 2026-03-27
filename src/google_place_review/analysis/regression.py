from __future__ import annotations

from pathlib import Path

import pandas as pd
import statsmodels.api as sm

from .visualization import save_horizontal_bar_chart
from .visualization import save_scatter_with_regression_line


ASPECT_FEATURES = [
    "飲品",
    "正餐",
    "甜點",
    "功能用途",
    "氛圍/風格",
    "店員/服務",
    "價格",
    "衛生/整潔",
    "出餐速度",
]


def build_review_feature_frame(sentiment_df: pd.DataFrame, aspect_df: pd.DataFrame) -> pd.DataFrame:
    base_columns = [
        "place_id",
        "place_name",
        "review_unique_key",
        "star_rating",
        "sentiment_score",
        "review_text",
        "likes_count",
        "review_date_text",
        "review_date_estimated",
    ]
    base_df = sentiment_df[base_columns].drop_duplicates(subset=["review_unique_key"]).copy()
    if aspect_df.empty:
        review_features = base_df.copy()
        for feature_name in ASPECT_FEATURES:
            review_features[feature_name] = 0.0
        return review_features

    pivot_df = (
        aspect_df.pivot_table(
            index="review_unique_key",
            columns="aspect_name",
            values="aspect_sentiment_score",
            aggfunc="mean",
        )
        .reset_index()
        .fillna(0.0)
    )
    pivot_df.columns.name = None
    review_features = base_df.merge(pivot_df, on="review_unique_key", how="left")
    for feature_name in ASPECT_FEATURES:
        if feature_name not in review_features.columns:
            review_features[feature_name] = 0.0
        review_features[feature_name] = pd.to_numeric(review_features[feature_name], errors="coerce").fillna(0.0)
    return review_features


def compute_aspect_star_relation(review_features: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    scopes: list[tuple[str, str | None]] = [("all", None)]
    scopes.extend(("place", place_id) for place_id in sorted(review_features["place_id"].dropna().unique()))

    for scope_name, place_id in scopes:
        scope_df = review_features.copy()
        if place_id is not None:
            scope_df = scope_df[scope_df["place_id"] == place_id].copy()
        scope_df = scope_df.dropna(subset=["star_rating"])
        if scope_df.empty:
            continue

        overall_subset = scope_df[["star_rating", "sentiment_score"]].dropna()
        rows.append(
            {
                "scope": scope_name,
                "place_id": place_id or "all",
                "aspect_name": "整體情緒",
                "sample_count": len(overall_subset),
                "pearson_r": _safe_corr(overall_subset["star_rating"], overall_subset["sentiment_score"], method="pearson"),
                "spearman_r": _safe_corr(overall_subset["star_rating"], overall_subset["sentiment_score"], method="spearman"),
            }
        )

        for feature_name in ASPECT_FEATURES:
            mentioned_df = scope_df[scope_df[feature_name] != 0].copy()
            relation_df = mentioned_df[["star_rating", feature_name]].dropna()
            if len(relation_df) < 5:
                rows.append(
                    {
                        "scope": scope_name,
                        "place_id": place_id or "all",
                        "aspect_name": feature_name,
                        "sample_count": len(relation_df),
                        "pearson_r": None,
                        "spearman_r": None,
                    }
                )
                continue
            rows.append(
                {
                    "scope": scope_name,
                    "place_id": place_id or "all",
                    "aspect_name": feature_name,
                    "sample_count": len(relation_df),
                    "pearson_r": _safe_corr(relation_df["star_rating"], relation_df[feature_name], method="pearson"),
                    "spearman_r": _safe_corr(relation_df["star_rating"], relation_df[feature_name], method="spearman"),
                }
            )
    return pd.DataFrame(rows)


def run_review_level_regression(review_features: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict] = []
    coefficient_rows: list[dict] = []
    scopes: list[tuple[str, str | None]] = [("all", None)]
    scopes.extend(("place", place_id) for place_id in sorted(review_features["place_id"].dropna().unique()))

    for scope_name, place_id in scopes:
        scope_df = review_features.copy()
        if place_id is not None:
            scope_df = scope_df[scope_df["place_id"] == place_id].copy()
        scope_df = scope_df.dropna(subset=["star_rating"])
        if scope_df.empty:
            continue

        _fit_model(
            scope_df=scope_df,
            scope_name=scope_name,
            place_id=place_id or "all",
            model_name="star_on_overall_sentiment",
            features=["sentiment_score"],
            summary_rows=summary_rows,
            coefficient_rows=coefficient_rows,
        )

        aspect_features = [feature for feature in ASPECT_FEATURES if scope_df[feature].nunique(dropna=True) > 1]
        if aspect_features:
            _fit_model(
                scope_df=scope_df,
                scope_name=scope_name,
                place_id=place_id or "all",
                model_name="star_on_aspect_sentiments",
                features=aspect_features,
                summary_rows=summary_rows,
                coefficient_rows=coefficient_rows,
            )

    return pd.DataFrame(summary_rows), pd.DataFrame(coefficient_rows)


def save_regression_figures(
    *,
    review_features: pd.DataFrame,
    aspect_relation_df: pd.DataFrame,
    figures_dir: Path,
) -> None:
    figures_dir.mkdir(parents=True, exist_ok=True)

    pooled_aspects = aspect_relation_df[
        (aspect_relation_df["scope"] == "all") & (aspect_relation_df["aspect_name"] != "整體情緒")
    ].dropna(subset=["pearson_r"])
    if not pooled_aspects.empty:
        plot_df = (
            pooled_aspects[["aspect_name", "pearson_r"]]
            .sort_values("pearson_r", ascending=True)
            .rename(columns={"aspect_name": "面向", "pearson_r": "Pearson correlation"})
        )
        save_horizontal_bar_chart(
            plot_df,
            x="Pearson correlation",
            y="面向",
            title="各面向 sentiment 與星數的 Pearson 關聯",
            output_path=figures_dir / "aspect_star_correlation.png",
        )

    scatter_df = review_features.dropna(subset=["star_rating", "sentiment_score"])
    if not scatter_df.empty:
        save_scatter_with_regression_line(
            scatter_df,
            x="sentiment_score",
            y="star_rating",
            title="整體 sentiment 與星數的關聯",
            output_path=figures_dir / "sentiment_star_scatter.png",
        )


def _fit_model(
    *,
    scope_df: pd.DataFrame,
    scope_name: str,
    place_id: str,
    model_name: str,
    features: list[str],
    summary_rows: list[dict],
    coefficient_rows: list[dict],
) -> None:
    model_df = scope_df[["star_rating", *features]].dropna().copy()
    if len(model_df) < max(20, len(features) * 5):
        summary_rows.append(
            {
                "model_name": model_name,
                "scope": scope_name,
                "place_id": place_id,
                "n_obs": len(model_df),
                "r_squared": None,
                "adj_r_squared": None,
                "status": "insufficient_sample",
            }
        )
        return

    x_df = sm.add_constant(model_df[features], has_constant="add")
    y_series = model_df["star_rating"]

    try:
        model = sm.OLS(y_series, x_df).fit()
    except Exception as exc:
        summary_rows.append(
            {
                "model_name": model_name,
                "scope": scope_name,
                "place_id": place_id,
                "n_obs": len(model_df),
                "r_squared": None,
                "adj_r_squared": None,
                "status": f"fit_failed:{type(exc).__name__}",
            }
        )
        return

    summary_rows.append(
        {
            "model_name": model_name,
            "scope": scope_name,
            "place_id": place_id,
            "n_obs": int(model.nobs),
            "r_squared": float(model.rsquared),
            "adj_r_squared": float(model.rsquared_adj),
            "status": "ok",
        }
    )
    for feature_name in model.params.index:
        coefficient_rows.append(
            {
                "model_name": model_name,
                "scope": scope_name,
                "place_id": place_id,
                "feature_name": feature_name,
                "coef": float(model.params[feature_name]),
                "p_value": float(model.pvalues[feature_name]),
            }
        )


def _safe_corr(series_x: pd.Series, series_y: pd.Series, *, method: str) -> float | None:
    if len(series_x) < 2 or len(series_y) < 2:
        return None
    if series_x.nunique(dropna=True) <= 1 or series_y.nunique(dropna=True) <= 1:
        return None
    value = series_x.corr(series_y, method=method)
    if pd.isna(value):
        return None
    return float(value)
