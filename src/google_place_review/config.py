from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ScraperConfig:
    target_url: str
    headless: bool = True
    scroll_strategy: str = "baseline"
    max_scroll_rounds: int = 60
    max_idle_rounds: int = 5
    max_reviews: int | None = None
    scroll_pause_min_seconds: float = 2.0
    scroll_pause_max_seconds: float = 4.5
    action_pause_min_seconds: float = 1.0
    action_pause_max_seconds: float = 2.5
    output_dir: Path = Path("data/raw")
    place_id_override: str | None = None
    chromedriver_path: Path | None = None
    debug_save_html: bool = False
    page_load_timeout_seconds: int = 60
    panel_interaction_mode: str = "pagedown"
    panel_focus_before_scroll: bool = True
