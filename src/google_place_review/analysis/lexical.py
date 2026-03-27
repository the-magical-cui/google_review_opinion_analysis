from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import jieba
import pandas as pd

from .dictionaries import STOPWORDS
from .preprocessing import normalize_text
from .sentiment import score_text_sentiment


_USER_DICT_LOADED = False
EXTRA_STOPWORDS = {"類型", "平均", "平均每人消費金額", "每人消費金額"}


def _ensure_user_dict_loaded() -> None:
    global _USER_DICT_LOADED
    if _USER_DICT_LOADED:
        return
    user_dict_path = Path(__file__).with_name("user_dict.txt")
    if user_dict_path.exists():
        jieba.load_userdict(str(user_dict_path))
    _USER_DICT_LOADED = True


def tokenize_text(text: str | None) -> list[str]:
    _ensure_user_dict_loaded()
    normalized = normalize_text(text)
    if not normalized:
        return []
    tokens: list[str] = []
    for token in jieba.lcut(normalized):
        cleaned = token.strip().lower()
        if len(cleaned) < 2:
            continue
        if cleaned in STOPWORDS:
            continue
        if cleaned in EXTRA_STOPWORDS:
            continue
        if cleaned.isdigit():
            continue
        if not re.search(r"[a-zA-Z\u4e00-\u9fff]", cleaned):
            continue
        tokens.append(cleaned)
    return tokens


def run_lexical_analysis(df: pd.DataFrame, *, group_col: str = "place_id", top_n: int = 20) -> pd.DataFrame:
    rows: list[dict] = []
    for group_value, group_df in df.groupby(group_col):
        counter: Counter[str] = Counter()
        for text in group_df["review_text"].fillna(""):
            counter.update(tokenize_text(text))
        for rank, (term, count) in enumerate(counter.most_common(top_n), start=1):
            rows.append(
                {
                    group_col: group_value,
                    "term": term,
                    "term_count": count,
                    "rank": rank,
                }
            )
    return pd.DataFrame(rows)


def run_collocation_analysis(
    df: pd.DataFrame,
    top_terms_df: pd.DataFrame,
    *,
    group_col: str = "place_id",
    context_window: int = 10,
) -> pd.DataFrame:
    rows: list[dict] = []
    if df.empty or top_terms_df.empty:
        return pd.DataFrame(
            columns=[
                group_col,
                "term",
                "rank",
                "mention_count",
                "avg_context_sentiment",
                "positive_context_count",
                "negative_context_count",
                "positive_ratio",
                "negative_ratio",
                "top_positive_review_text",
                "top_positive_review_likes",
                "top_negative_review_text",
                "top_negative_review_likes",
            ]
        )

    for group_value, terms_group in top_terms_df.groupby(group_col):
        group_reviews = df.loc[df[group_col] == group_value].copy()
        for _, term_row in terms_group.iterrows():
            term = str(term_row.get("term", "")).strip()
            if not term:
                continue

            context_scores: list[float] = []
            mention_count = 0
            positive_candidates: list[dict] = []
            negative_candidates: list[dict] = []

            for review_row in group_reviews.itertuples(index=False):
                text = str(getattr(review_row, "review_text", "") or "")
                normalized = normalize_text(text)
                if not normalized or term not in normalized:
                    continue

                start_index = 0
                while True:
                    hit_index = normalized.find(term, start_index)
                    if hit_index < 0:
                        break
                    left = max(0, hit_index - context_window)
                    right = min(len(normalized), hit_index + len(term) + context_window)
                    context = normalized[left:right].strip()
                    mention_count += 1
                    if context:
                        score = score_text_sentiment(context)
                        context_scores.append(score)
                        candidate = {
                            "score": score,
                            "likes_count": int(getattr(review_row, "likes_count", 0) or 0),
                            "review_date_estimated": str(getattr(review_row, "review_date_estimated", "") or ""),
                            "review_text": text,
                        }
                        if score > 0.5:
                            positive_candidates.append(candidate)
                        elif score < -0.5:
                            negative_candidates.append(candidate)
                    start_index = hit_index + len(term)

            if mention_count == 0:
                continue

            positive_context_count = sum(1 for score in context_scores if score > 0.5)
            negative_context_count = sum(1 for score in context_scores if score < -0.5)
            avg_context_sentiment = sum(context_scores) / len(context_scores) if context_scores else 0.0
            top_positive = _pick_representative_review(positive_candidates)
            top_negative = _pick_representative_review(negative_candidates)
            rows.append(
                {
                    group_col: group_value,
                    "term": term,
                    "rank": int(term_row.get("rank", 0) or 0),
                    "mention_count": mention_count,
                    "avg_context_sentiment": avg_context_sentiment,
                    "positive_context_count": positive_context_count,
                    "negative_context_count": negative_context_count,
                    "positive_ratio": positive_context_count / mention_count if mention_count else 0.0,
                    "negative_ratio": negative_context_count / mention_count if mention_count else 0.0,
                    "top_positive_review_text": top_positive.get("review_text", ""),
                    "top_positive_review_likes": top_positive.get("likes_count"),
                    "top_negative_review_text": top_negative.get("review_text", ""),
                    "top_negative_review_likes": top_negative.get("likes_count"),
                }
            )

    return pd.DataFrame(rows)


def _pick_representative_review(candidates: list[dict]) -> dict:
    if not candidates:
        return {}
    return max(
        candidates,
        key=lambda item: (
            int(item.get("likes_count", 0) or 0),
            str(item.get("review_date_estimated", "") or ""),
        ),
    )
