from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class ReviewRecord:
    place_id: str
    place_name: str | None
    source_url: str
    reviewer_name: str | None
    review_text: str | None
    star_rating: float | None
    review_date_text: str | None
    owner_response_text: str | None
    owner_response_date: str | None
    scraped_at: str
    review_unique_key: str
    review_date_estimated: str | None
    scrape_run_id: str
    review_language: str | None = None
    reviewer_profile_url: str | None = None
    likes_count: int | None = None
    raw_review_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ScrapeResult:
    place_id: str
    place_name: str | None
    scrape_run_id: str
    total_reviews: int
    new_reviews_added: int
    latest_output_path: Path
    run_output_path: Path
