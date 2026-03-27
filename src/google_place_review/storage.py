from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

from google_place_review.models import ReviewRecord


def ensure_place_paths(base_dir: Path, place_id: str) -> dict[str, Path]:
    place_dir = base_dir / place_id
    runs_dir = place_dir / "runs"
    place_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)
    return {
        "place_dir": place_dir,
        "runs_dir": runs_dir,
        "latest_jsonl": place_dir / "reviews_latest.jsonl",
        "meta_json": place_dir / "meta.json",
    }


def load_existing_identities(latest_jsonl: Path) -> set[str]:
    if not latest_jsonl.exists():
        return set()

    identities: set[str] = set()
    with latest_jsonl.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            identity = _identity_from_payload(payload)
            if identity:
                identities.add(identity)
    return identities


def write_jsonl(path: Path, reviews: list[ReviewRecord]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for review in reviews:
            fh.write(json.dumps(review.to_dict(), ensure_ascii=False) + "\n")


def upsert_latest_reviews(path: Path, reviews: list[ReviewRecord]) -> int:
    merged: dict[str, ReviewRecord] = {}
    order: list[str] = []

    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                    record = ReviewRecord(**payload)
                except (json.JSONDecodeError, TypeError):
                    continue
                _merge_record_into_store(record, merged, order)

    existing_identity_count = len(merged)
    for review in reviews:
        _merge_record_into_store(review, merged, order)

    with path.open("w", encoding="utf-8") as fh:
        for identity in order:
            review = merged.get(identity)
            if review is None:
                continue
            fh.write(json.dumps(review.to_dict(), ensure_ascii=False) + "\n")

    return max(0, len(merged) - existing_identity_count)


def write_meta(
    path: Path,
    *,
    place_id: str,
    place_name: str | None,
    source_url: str,
    scrape_run_id: str,
    total_reviews_in_latest: int,
    reviews_in_run: int,
) -> None:
    payload = {
        "place_id": place_id,
        "place_name": place_name,
        "source_url": source_url,
        "last_scrape_run_id": scrape_run_id,
        "total_reviews_in_latest": total_reviews_in_latest,
        "reviews_in_run": reviews_in_run,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _merge_record_into_store(record: ReviewRecord, merged: dict[str, ReviewRecord], order: list[str]) -> None:
    identity = _identity_from_record(record)
    if identity in merged:
        merged[identity] = _prefer_review_record(merged[identity], record)
        return

    alias_identity = _find_merge_candidate_identity(record, merged)
    if alias_identity is not None:
        merged[alias_identity] = _prefer_review_record(merged[alias_identity], record)
        return

    merged[identity] = record
    order.append(identity)


def _find_merge_candidate_identity(record: ReviewRecord, merged: dict[str, ReviewRecord]) -> str | None:
    for identity, existing in merged.items():
        if _records_should_merge(existing, record):
            return identity
    return None


def _records_should_merge(left: ReviewRecord, right: ReviewRecord) -> bool:
    if left.place_id != right.place_id:
        return False

    left_review_id = (left.raw_review_metadata or {}).get("review_id")
    right_review_id = (right.raw_review_metadata or {}).get("review_id")
    if left_review_id and right_review_id:
        return left_review_id == right_review_id

    left_text = _normalized_review_text(left.review_text)
    right_text = _normalized_review_text(right.review_text)
    if not left_text or not right_text:
        return False

    min_len = min(len(left_text), len(right_text))
    if min_len < 24:
        return False
    text_overlaps = left_text.startswith(right_text) or right_text.startswith(left_text)
    if not text_overlaps:
        return False

    if _is_low_quality_record(left) or _is_low_quality_record(right):
        return True

    if not _compatible_optional_text(left.reviewer_name, right.reviewer_name):
        return False
    if not _compatible_optional_text(left.review_date_text, right.review_date_text):
        return False
    if not _compatible_optional_number(left.star_rating, right.star_rating):
        return False
    return True


def _is_low_quality_record(record: ReviewRecord) -> bool:
    metadata = record.raw_review_metadata or {}
    return (
        not metadata.get("review_id")
        and not record.reviewer_name
        and not record.review_date_text
        and record.star_rating is None
    )


def _compatible_optional_text(left: str | None, right: str | None) -> bool:
    left_norm = _normalize_text(left)
    right_norm = _normalize_text(right)
    return not left_norm or not right_norm or left_norm == right_norm


def _compatible_optional_number(left: float | None, right: float | None) -> bool:
    return left is None or right is None or left == right


def _identity_from_payload(payload: dict) -> str | None:
    raw_review_metadata = payload.get("raw_review_metadata") or {}
    review_id = raw_review_metadata.get("review_id")
    place_id = payload.get("place_id") or ""
    if review_id:
        return f"{place_id}::review_id::{review_id}"
    review_unique_key = payload.get("review_unique_key")
    if review_unique_key:
        return f"{place_id}::review_key::{review_unique_key}"
    return None


def _identity_from_record(record: ReviewRecord) -> str:
    review_id = (record.raw_review_metadata or {}).get("review_id")
    if review_id:
        return f"{record.place_id}::review_id::{review_id}"
    return f"{record.place_id}::review_key::{record.review_unique_key}"


def _prefer_review_record(left: ReviewRecord, right: ReviewRecord) -> ReviewRecord:
    left_score = _record_quality_score(left)
    right_score = _record_quality_score(right)
    if right_score > left_score:
        return right
    if right_score < left_score:
        return left
    return right if right.scraped_at >= left.scraped_at else left


def _record_quality_score(record: ReviewRecord) -> tuple[int, int, int, int, int, str]:
    review_text = record.review_text or ""
    metadata = record.raw_review_metadata or {}
    return (
        1 if record.review_date_text else 0,
        1 if record.reviewer_name else 0,
        1 if record.star_rating is not None else 0,
        len(review_text),
        1 if metadata.get("review_id") else 0,
        record.scraped_at,
    )


def _normalized_review_text(value: str | None) -> str:
    normalized = _normalize_text(value)
    for suffix in ["...", "..", ".", "?"]:
        while normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)].rstrip()
    return normalized[:180]


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKC", value)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip().lower()
