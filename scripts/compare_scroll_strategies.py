from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_URL = "https://www.google.com/maps/place/Rebirth/@25.0213738,121.5303775,17z/data=!4m8!3m7!1s0x3442a988e919f47d:0xc36d3628b077d501!8m2!3d25.021369!4d121.5329524!9m1!1b1!16s%2Fg%2F1ptvr89ww?entry=ttu&g_ep=EgoyMDI2MDMxMS4wIKXMDSoASAFQAw%3D%3D"
FOLLOWUP_STRATEGIES = ["scroll_into_view", "action_chains"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="?? Google Maps reviews ????")
    parser.add_argument("--url", default=DEFAULT_URL, help="??? Google Maps place URL")
    parser.add_argument("--chromedriver-path", required=True, help="chromedriver ??")
    parser.add_argument("--max-scroll-rounds", type=int, default=40)
    parser.add_argument("--max-idle-rounds", type=int, default=8)
    parser.add_argument("--max-reviews", type=int, default=50)
    parser.add_argument("--headless", action="store_true", help="?? headless")
    return parser.parse_args()


def build_command(args: argparse.Namespace, strategy: str) -> list[str]:
    command = [
        sys.executable,
        "scripts/run_scraper.py",
        "--url",
        args.url,
        "--scroll-strategy",
        strategy,
        "--chromedriver-path",
        args.chromedriver_path,
        "--max-scroll-rounds",
        str(args.max_scroll_rounds),
        "--max-idle-rounds",
        str(args.max_idle_rounds),
        "--max-reviews",
        str(args.max_reviews),
    ]
    if not args.headless:
        command.append("--no-headless")
    return command


def run_strategy(args: argparse.Namespace, strategy: str) -> tuple[int, int | None]:
    print(f"\n=== strategy: {strategy} ===", flush=True)
    completed = subprocess.run(build_command(args, strategy), cwd=PROJECT_ROOT, capture_output=True, text=True)
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    match = re.search(r"total_reviews:\s*(\d+)", completed.stdout or "")
    total_reviews = int(match.group(1)) if match else None
    return completed.returncode, total_reviews


def main() -> int:
    args = parse_args()
    exit_code = 0
    baseline_code, baseline_total = run_strategy(args, "baseline")
    exit_code = baseline_code
    if baseline_code != 0:
        return exit_code
    if baseline_total is None or baseline_total <= 18:
        print("\nSkipping follow-up strategies because baseline did not exceed 18 reviews.", flush=True)
        return exit_code
    for strategy in FOLLOWUP_STRATEGIES:
        result_code, _ = run_strategy(args, strategy)
        if result_code != 0:
            exit_code = result_code
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
