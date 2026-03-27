from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from .aspects import summarize_aspect_sentiment
from .lexical import tokenize_text
from .sentiment import summarize_sentiment


def run_cross_store_comparison(sentiment_df: pd.DataFrame, aspect_df: pd.DataFrame, *, min_mentions: int = 5) -> dict[str, pd.DataFrame]:
    sentiment_summary = summarize_sentiment(sentiment_df, group_col="place_id")
    place_names = sentiment_df[["place_id", "place_name"]].drop_duplicates()
    sentiment_summary = place_names.merge(sentiment_summary, on="place_id", how="right")
    aspect_summary = summarize_aspect_sentiment(aspect_df)
    if not aspect_summary.empty:
        aspect_summary = aspect_summary[aspect_summary["mention_count"] >= min_mentions].copy()
        aspect_summary = place_names.merge(aspect_summary, on="place_id", how="right")
    return {
        "sentiment_comparison": sentiment_summary,
        "aspect_comparison": aspect_summary,
    }


def run_tfidf_comparison(df: pd.DataFrame, *, top_n: int = 15) -> pd.DataFrame:
    docs = (
        df.groupby(["place_id", "place_name"])["review_text"]
        .apply(lambda series: " ".join(" ".join(tokenize_text(text)) for text in series.fillna("")))
        .reset_index()
    )
    docs = docs[docs["review_text"].str.strip() != ""].copy()
    if docs.empty:
        return pd.DataFrame(columns=["place_id", "place_name", "term", "tfidf_score", "rank"])

    vectorizer = TfidfVectorizer(token_pattern=r"(?u)\b\w+\b")
    matrix = vectorizer.fit_transform(docs["review_text"])
    terms = vectorizer.get_feature_names_out()

    rows: list[dict] = []
    for index, record in docs.iterrows():
        scores = matrix[index].toarray().ravel()
        top_indices = scores.argsort()[::-1][:top_n]
        for rank, term_index in enumerate(top_indices, start=1):
            score = float(scores[term_index])
            if score <= 0:
                continue
            rows.append(
                {
                    "place_id": record["place_id"],
                    "place_name": record["place_name"],
                    "term": terms[term_index],
                    "tfidf_score": score,
                    "rank": rank,
                }
            )
    return pd.DataFrame(rows)
