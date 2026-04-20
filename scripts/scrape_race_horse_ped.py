#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Iterable, List, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from modules import preparing


RACE_DIR = ROOT_DIR / "data" / "html" / "race"
HORSE_DIR = ROOT_DIR / "data" / "html" / "horse"
PED_DIR = ROOT_DIR / "data" / "html" / "ped"
TMP_DIR = ROOT_DIR / "data" / "tmp"
SUMMARY_PATH = TMP_DIR / "scrape_race_horse_ped_summary.json"

HORSE_ID_PATTERNS = [
    re.compile(rb'https?://db\.netkeiba\.com/horse/([0-9]{6,12})/?'),
    re.compile(rb'href=["\'](?:https?://db\.netkeiba\.com)?/horse/([0-9]{6,12})/?["\']'),
    re.compile(rb'/horse/([0-9]{6,12})/?'),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape only race/ horse/ ped HTML files and push each successful update immediately."
    )
    parser.add_argument("--from-date", required=True, help="Start date, e.g. 2010/01/01")
    parser.add_argument("--to-date", required=True, help="End date, e.g. 2026/04/20")
    parser.add_argument(
        "--overwrite-html",
        action="store_true",
        help="Re-download existing html files instead of skipping them.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.2,
        help="Sleep time between scrape requests.",
    )
    parser.add_argument(
        "--git-push",
        action="store_true",
        help="Commit/push every detected change immediately.",
    )
    return parser.parse_args()


def run(cmd: List[str], *, check: bool = True, capture_output: bool = False) -> str:
    proc = subprocess.run(
        cmd,
        check=check,
        text=True,
        capture_output=capture_output,
    )
    return proc.stdout if capture_output else ""


def ensure_dirs() -> None:
    for d in (RACE_DIR, HORSE_DIR, PED_DIR, TMP_DIR):
        d.mkdir(parents=True, exist_ok=True)


def git_has_changes() -> bool:
    out = run(["git", "status", "--porcelain"], capture_output=True)
    return bool(out.strip())


def git_commit_and_push(message: str) -> bool:
    run(["git", "add", "-A"])
    if not git_has_changes():
        return False

    branch = os.getenv("GITHUB_REF_NAME")
    if not branch:
        branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True).strip()

    run(["git", "commit", "-m", message])
    run(["git", "pull", "--rebase", "origin", branch])
    run(["git", "push", "origin", f"HEAD:{branch}"])
    return True


def dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for value in values:
        value = str(value).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def normalize_date_string(value: str) -> str:
    value = str(value).strip()
    digits = re.sub(r'\D', '', value)
    if len(digits) == 8:
        return digits
    for fmt in ('%Y/%m/%d', '%Y-%m-%d', '%Y%m%d'):
        try:
            return dt.datetime.strptime(value, fmt).strftime('%Y%m%d')
        except ValueError:
            continue
    raise ValueError(f'Unsupported date format: {value}')


def extract_horse_ids_from_race_file(path: Path) -> List[str]:
    if not path.exists():
        return []

    content = path.read_bytes()
    ids: List[str] = []
    seen = set()
    for pattern in HORSE_ID_PATTERNS:
        for match in pattern.finditer(content):
            horse_id = match.group(1).decode("utf-8", errors="ignore")
            if horse_id not in seen:
                seen.add(horse_id)
                ids.append(horse_id)
    return ids


def write_summary(summary: dict) -> None:
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def scrape_race_ids(from_date: str, to_date: str) -> Tuple[List[str], str, int]:
    print(f"[INFO] scrape_kaisai_date: {from_date} -> {to_date}", flush=True)
    kaisai_dates_raw = preparing.scrape_kaisai_date(from_=from_date, to_=to_date)
    kaisai_dates = dedupe_keep_order(kaisai_dates_raw)
    normalized_kaisai_dates = [normalize_date_string(value) for value in kaisai_dates]
    latest_kaisai_date = max(normalized_kaisai_dates)
    kaisai_dates_upto_latest = [value for value, normalized in zip(kaisai_dates, normalized_kaisai_dates) if normalized <= latest_kaisai_date]

    print(f"[INFO] kaisai_date count = {len(kaisai_dates_upto_latest)}", flush=True)
    print(f"[INFO] latest_kaisai_date = {latest_kaisai_date}", flush=True)

    print("[INFO] scrape_race_id_list", flush=True)
    race_ids = preparing.scrape_race_id_list(kaisai_dates_upto_latest)
    race_ids = dedupe_keep_order(race_ids)
    print(f"[INFO] race_id count = {len(race_ids)}", flush=True)
    return race_ids, latest_kaisai_date, len(kaisai_dates_upto_latest)


def main() -> int:
    args = parse_args()
    ensure_dirs()
    skip_existing = not args.overwrite_html

    summary = {
        "from_date": args.from_date,
        "to_date_requested": args.to_date,
        "latest_kaisai_date": None,
        "kaisai_date_count": 0,
        "skip_existing_duplicates": skip_existing,
        "overwrite_html": args.overwrite_html,
        "sleep_seconds": args.sleep_seconds,
        "race": {"attempted": 0, "pushed": 0, "failed": []},
        "horse": {"attempted": 0, "pushed": 0, "failed": []},
        "ped": {"attempted": 0, "pushed": 0, "failed": []},
    }
    write_summary(summary)

    race_ids, latest_kaisai_date, kaisai_date_count = scrape_race_ids(args.from_date, args.to_date)
    summary["latest_kaisai_date"] = latest_kaisai_date
    summary["kaisai_date_count"] = kaisai_date_count
    write_summary(summary)

    horse_ids: List[str] = []

    for i, race_id in enumerate(race_ids, start=1):
        summary["race"]["attempted"] = i
        race_path = RACE_DIR / f"{race_id}.bin"
        print(f"[RACE {i}/{len(race_ids)}] {race_id}", flush=True)
        try:
            preparing.scrape_html_race([race_id], skip=skip_existing)
            horse_ids.extend(extract_horse_ids_from_race_file(race_path))

            if args.git_push and git_commit_and_push(f"chore: scrape race html {race_id}"):
                summary["race"]["pushed"] += 1
                print(f"[PUSH] race {race_id}", flush=True)
            elif args.git_push:
                print(f"[SKIP PUSH] no diff for race {race_id}", flush=True)
        except Exception:
            error_text = traceback.format_exc(limit=3)
            summary["race"]["failed"].append({"id": race_id, "error": error_text})
            print(error_text, file=sys.stderr, flush=True)
        finally:
            write_summary(summary)
            time.sleep(args.sleep_seconds)

    horse_ids = dedupe_keep_order(horse_ids)

    if not horse_ids:
        print("[WARN] horse_id extraction from race html returned 0. fallback to scrape_horse_id_list().", flush=True)
        try:
            horse_ids = dedupe_keep_order(preparing.scrape_horse_id_list(race_ids))
        except Exception:
            error_text = traceback.format_exc(limit=3)
            summary["horse"]["failed"].append({"id": "fallback_horse_id_list", "error": error_text})
            print(error_text, file=sys.stderr, flush=True)
            write_summary(summary)
            horse_ids = []

    print(f"[INFO] horse_id count = {len(horse_ids)}", flush=True)

    for i, horse_id in enumerate(horse_ids, start=1):
        summary["horse"]["attempted"] = i
        print(f"[HORSE {i}/{len(horse_ids)}] {horse_id}", flush=True)
        try:
            preparing.scrape_html_horse_with_master([horse_id], skip=skip_existing)

            if args.git_push and git_commit_and_push(f"chore: scrape horse html {horse_id}"):
                summary["horse"]["pushed"] += 1
                print(f"[PUSH] horse {horse_id}", flush=True)
            elif args.git_push:
                print(f"[SKIP PUSH] no diff for horse {horse_id}", flush=True)
        except Exception:
            error_text = traceback.format_exc(limit=3)
            summary["horse"]["failed"].append({"id": horse_id, "error": error_text})
            print(error_text, file=sys.stderr, flush=True)
        finally:
            write_summary(summary)
            time.sleep(args.sleep_seconds)

    for i, horse_id in enumerate(horse_ids, start=1):
        summary["ped"]["attempted"] = i
        print(f"[PED {i}/{len(horse_ids)}] {horse_id}", flush=True)
        try:
            preparing.scrape_html_ped([horse_id], skip=skip_existing)

            if args.git_push and git_commit_and_push(f"chore: scrape ped html {horse_id}"):
                summary["ped"]["pushed"] += 1
                print(f"[PUSH] ped {horse_id}", flush=True)
            elif args.git_push:
                print(f"[SKIP PUSH] no diff for ped {horse_id}", flush=True)
        except Exception:
            error_text = traceback.format_exc(limit=3)
            summary["ped"]["failed"].append({"id": horse_id, "error": error_text})
            print(error_text, file=sys.stderr, flush=True)
        finally:
            write_summary(summary)
            time.sleep(args.sleep_seconds)

    write_summary(summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
