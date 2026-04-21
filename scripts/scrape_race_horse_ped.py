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
STATE_DIR = ROOT_DIR / ".github" / "state"
SUMMARY_PATH = TMP_DIR / "scrape_race_horse_ped_summary.json"
CHECKPOINT_PATH = STATE_DIR / "scrape_race_horse_ped_checkpoint.json"
CHECKPOINT_RELATIVE_PATH = str(CHECKPOINT_PATH.relative_to(ROOT_DIR))
GIT_COMMIT_TARGETS = [
    str(RACE_DIR.relative_to(ROOT_DIR)),
    str(HORSE_DIR.relative_to(ROOT_DIR)),
    str(PED_DIR.relative_to(ROOT_DIR)),
    CHECKPOINT_RELATIVE_PATH,
]
CHECKPOINT_PUSH_EVERY = 100
CHECKPOINT_PUSH_INTERVAL_SECONDS = 60

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


def now_jst_iso() -> str:
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("Asia/Tokyo")).isoformat()
    except Exception:
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=9))).isoformat()


def ensure_dirs() -> None:
    for d in (RACE_DIR, HORSE_DIR, PED_DIR, TMP_DIR, STATE_DIR):
        d.mkdir(parents=True, exist_ok=True)


def get_staged_files() -> List[str]:
    out = run(["git", "diff", "--cached", "--name-only"], capture_output=True)
    return [line.strip() for line in out.splitlines() if line.strip()]


def git_commit_and_push(message: str, *, allow_checkpoint_only: bool) -> bool:
    run(["git", "add", *GIT_COMMIT_TARGETS])
    staged_files = get_staged_files()
    if not staged_files:
        return False

    non_checkpoint_files = [
        path for path in staged_files if path != CHECKPOINT_RELATIVE_PATH
    ]
    if not allow_checkpoint_only and not non_checkpoint_files:
        return False

    branch = os.getenv("GITHUB_REF_NAME")
    if not branch:
        branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True).strip()

    run(["git", "commit", "-m", message])
    run(["git", "pull", "--rebase", "--autostash", "origin", branch])
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


def get_candidate_paths(base_dir: Path, item_id: str) -> List[Path]:
    item_id = str(item_id).strip()
    return [
        base_dir / f"{item_id}.bin",
        base_dir / f"{item_id}.html",
        base_dir / item_id,
        base_dir / item_id / "index.html",
        base_dir / item_id / "index.bin",
    ]


def has_existing_scrape_file(base_dir: Path, item_id: str) -> bool:
    return any(path.exists() for path in get_candidate_paths(base_dir, item_id))


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


def collect_horse_ids_from_race_files(race_ids: Iterable[str]) -> List[str]:
    horse_ids: List[str] = []
    for race_id in race_ids:
        race_path = RACE_DIR / f"{race_id}.bin"
        horse_ids.extend(extract_horse_ids_from_race_file(race_path))
    return dedupe_keep_order(horse_ids)


def write_summary(summary: dict) -> None:
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_checkpoint() -> dict | None:
    if not CHECKPOINT_PATH.exists():
        return None
    try:
        return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_checkpoint(checkpoint: dict) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.write_text(
        json.dumps(checkpoint, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def create_checkpoint(from_date: str, to_date_requested: str) -> dict:
    return {
        "version": 1,
        "from_date": from_date,
        "to_date_requested": to_date_requested,
        "latest_kaisai_date": None,
        "phase": "race",
        "race_last_id": None,
        "horse_last_id": None,
        "ped_last_id": None,
        "updated_at": now_jst_iso(),
    }


def resolve_checkpoint(from_date: str, to_date_requested: str) -> dict:
    checkpoint = load_checkpoint()
    if checkpoint is None:
        checkpoint = create_checkpoint(from_date, to_date_requested)
    else:
        if checkpoint.get("from_date") != from_date:
            checkpoint = create_checkpoint(from_date, to_date_requested)
        else:
            checkpoint["to_date_requested"] = to_date_requested
            checkpoint.setdefault("phase", "race")
            checkpoint.setdefault("race_last_id", None)
            checkpoint.setdefault("horse_last_id", None)
            checkpoint.setdefault("ped_last_id", None)
            checkpoint["updated_at"] = now_jst_iso()
    save_checkpoint(checkpoint)
    return checkpoint


def find_resume_index(items: List[str], last_id: str | None) -> int:
    if not last_id:
        return 0
    try:
        return items.index(str(last_id)) + 1
    except ValueError:
        return 0


def should_push_checkpoint(counter: int, last_push_at: float, *, force: bool = False) -> bool:
    if force:
        return counter > 0
    if counter >= CHECKPOINT_PUSH_EVERY:
        return True
    if counter > 0 and (time.time() - last_push_at) >= CHECKPOINT_PUSH_INTERVAL_SECONDS:
        return True
    return False


def update_checkpoint(checkpoint: dict, *, phase: str, latest_kaisai_date: str | None, item_id: str | None) -> None:
    checkpoint["phase"] = phase
    if latest_kaisai_date is not None:
        checkpoint["latest_kaisai_date"] = latest_kaisai_date
    if phase == "race":
        checkpoint["race_last_id"] = item_id
    elif phase == "horse":
        checkpoint["horse_last_id"] = item_id
    elif phase == "ped":
        checkpoint["ped_last_id"] = item_id
    checkpoint["updated_at"] = now_jst_iso()
    save_checkpoint(checkpoint)


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
    checkpoint = resolve_checkpoint(args.from_date, args.to_date)

    summary = {
        "from_date": args.from_date,
        "to_date_requested": args.to_date,
        "latest_kaisai_date": None,
        "kaisai_date_count": 0,
        "skip_existing_duplicates": skip_existing,
        "overwrite_html": args.overwrite_html,
        "sleep_seconds": args.sleep_seconds,
        "resume_checkpoint_path": CHECKPOINT_RELATIVE_PATH,
        "resume_phase": checkpoint.get("phase"),
        "resume_race_last_id": checkpoint.get("race_last_id"),
        "resume_horse_last_id": checkpoint.get("horse_last_id"),
        "resume_ped_last_id": checkpoint.get("ped_last_id"),
        "race": {"attempted": 0, "pushed": 0, "skipped_existing": 0, "failed": []},
        "horse": {"attempted": 0, "pushed": 0, "skipped_existing": 0, "failed": []},
        "ped": {"attempted": 0, "pushed": 0, "skipped_existing": 0, "failed": []},
    }
    write_summary(summary)

    race_ids, latest_kaisai_date, kaisai_date_count = scrape_race_ids(args.from_date, args.to_date)
    summary["latest_kaisai_date"] = latest_kaisai_date
    summary["kaisai_date_count"] = kaisai_date_count
    write_summary(summary)

    race_start_index = find_resume_index(race_ids, checkpoint.get("race_last_id"))
    print(f"[INFO] race resume index = {race_start_index}/{len(race_ids)}", flush=True)

    checkpoint_counter = 0
    last_checkpoint_push_at = time.time()

    for i, race_id in enumerate(race_ids[race_start_index:], start=race_start_index + 1):
        summary["race"]["attempted"] = i
        print(f"[RACE {i}/{len(race_ids)}] {race_id}", flush=True)
        try:
            if skip_existing and has_existing_scrape_file(RACE_DIR, race_id):
                summary["race"]["skipped_existing"] += 1
                update_checkpoint(checkpoint, phase="race", latest_kaisai_date=latest_kaisai_date, item_id=race_id)
                checkpoint_counter += 1
                if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at):
                    if git_commit_and_push(f"chore: checkpoint race {race_id}", allow_checkpoint_only=True):
                        checkpoint_counter = 0
                        last_checkpoint_push_at = time.time()
                        print(f"[PUSH] checkpoint race {race_id}", flush=True)
                print(f"[SKIP EXISTING] race {race_id}", flush=True)
                continue

            preparing.scrape_html_race([race_id], skip=skip_existing)
            update_checkpoint(checkpoint, phase="race", latest_kaisai_date=latest_kaisai_date, item_id=race_id)

            if args.git_push and git_commit_and_push(f"chore: scrape race html {race_id}", allow_checkpoint_only=False):
                summary["race"]["pushed"] += 1
                checkpoint_counter = 0
                last_checkpoint_push_at = time.time()
                print(f"[PUSH] race {race_id}", flush=True)
            else:
                checkpoint_counter += 1
                if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at):
                    if git_commit_and_push(f"chore: checkpoint race {race_id}", allow_checkpoint_only=True):
                        checkpoint_counter = 0
                        last_checkpoint_push_at = time.time()
                        print(f"[PUSH] checkpoint race {race_id}", flush=True)
                    else:
                        print(f"[SKIP PUSH] no diff for race {race_id}", flush=True)
                elif args.git_push:
                    print(f"[SKIP PUSH] no diff for race {race_id}", flush=True)
        except Exception:
            error_text = traceback.format_exc(limit=3)
            summary["race"]["failed"].append({"id": race_id, "error": error_text})
            print(error_text, file=sys.stderr, flush=True)
        finally:
            write_summary(summary)
            time.sleep(args.sleep_seconds)

    if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at, force=True):
        last_race_id = checkpoint.get("race_last_id")
        if last_race_id and git_commit_and_push(f"chore: checkpoint race {last_race_id}", allow_checkpoint_only=True):
            checkpoint_counter = 0
            last_checkpoint_push_at = time.time()
            print(f"[PUSH] checkpoint race {last_race_id}", flush=True)

    horse_ids = collect_horse_ids_from_race_files(race_ids)

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
    horse_start_index = find_resume_index(horse_ids, checkpoint.get("horse_last_id"))
    print(f"[INFO] horse resume index = {horse_start_index}/{len(horse_ids)}", flush=True)

    checkpoint_counter = 0
    last_checkpoint_push_at = time.time()

    for i, horse_id in enumerate(horse_ids[horse_start_index:], start=horse_start_index + 1):
        summary["horse"]["attempted"] = i
        print(f"[HORSE {i}/{len(horse_ids)}] {horse_id}", flush=True)
        try:
            if skip_existing and has_existing_scrape_file(HORSE_DIR, horse_id):
                summary["horse"]["skipped_existing"] += 1
                update_checkpoint(checkpoint, phase="horse", latest_kaisai_date=latest_kaisai_date, item_id=horse_id)
                checkpoint_counter += 1
                if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at):
                    if git_commit_and_push(f"chore: checkpoint horse {horse_id}", allow_checkpoint_only=True):
                        checkpoint_counter = 0
                        last_checkpoint_push_at = time.time()
                        print(f"[PUSH] checkpoint horse {horse_id}", flush=True)
                print(f"[SKIP EXISTING] horse {horse_id}", flush=True)
                continue

            preparing.scrape_html_horse_with_master([horse_id], skip=skip_existing)
            update_checkpoint(checkpoint, phase="horse", latest_kaisai_date=latest_kaisai_date, item_id=horse_id)

            if args.git_push and git_commit_and_push(f"chore: scrape horse html {horse_id}", allow_checkpoint_only=False):
                summary["horse"]["pushed"] += 1
                checkpoint_counter = 0
                last_checkpoint_push_at = time.time()
                print(f"[PUSH] horse {horse_id}", flush=True)
            else:
                checkpoint_counter += 1
                if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at):
                    if git_commit_and_push(f"chore: checkpoint horse {horse_id}", allow_checkpoint_only=True):
                        checkpoint_counter = 0
                        last_checkpoint_push_at = time.time()
                        print(f"[PUSH] checkpoint horse {horse_id}", flush=True)
                    else:
                        print(f"[SKIP PUSH] no diff for horse {horse_id}", flush=True)
                elif args.git_push:
                    print(f"[SKIP PUSH] no diff for horse {horse_id}", flush=True)
        except Exception:
            error_text = traceback.format_exc(limit=3)
            summary["horse"]["failed"].append({"id": horse_id, "error": error_text})
            print(error_text, file=sys.stderr, flush=True)
        finally:
            write_summary(summary)
            time.sleep(args.sleep_seconds)

    if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at, force=True):
        last_horse_id = checkpoint.get("horse_last_id")
        if last_horse_id and git_commit_and_push(f"chore: checkpoint horse {last_horse_id}", allow_checkpoint_only=True):
            checkpoint_counter = 0
            last_checkpoint_push_at = time.time()
            print(f"[PUSH] checkpoint horse {last_horse_id}", flush=True)

    ped_start_index = find_resume_index(horse_ids, checkpoint.get("ped_last_id"))
    print(f"[INFO] ped resume index = {ped_start_index}/{len(horse_ids)}", flush=True)

    checkpoint_counter = 0
    last_checkpoint_push_at = time.time()

    for i, horse_id in enumerate(horse_ids[ped_start_index:], start=ped_start_index + 1):
        summary["ped"]["attempted"] = i
        print(f"[PED {i}/{len(horse_ids)}] {horse_id}", flush=True)
        try:
            if skip_existing and has_existing_scrape_file(PED_DIR, horse_id):
                summary["ped"]["skipped_existing"] += 1
                update_checkpoint(checkpoint, phase="ped", latest_kaisai_date=latest_kaisai_date, item_id=horse_id)
                checkpoint_counter += 1
                if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at):
                    if git_commit_and_push(f"chore: checkpoint ped {horse_id}", allow_checkpoint_only=True):
                        checkpoint_counter = 0
                        last_checkpoint_push_at = time.time()
                        print(f"[PUSH] checkpoint ped {horse_id}", flush=True)
                print(f"[SKIP EXISTING] ped {horse_id}", flush=True)
                continue

            preparing.scrape_html_ped([horse_id], skip=skip_existing)
            update_checkpoint(checkpoint, phase="ped", latest_kaisai_date=latest_kaisai_date, item_id=horse_id)

            if args.git_push and git_commit_and_push(f"chore: scrape ped html {horse_id}", allow_checkpoint_only=False):
                summary["ped"]["pushed"] += 1
                checkpoint_counter = 0
                last_checkpoint_push_at = time.time()
                print(f"[PUSH] ped {horse_id}", flush=True)
            else:
                checkpoint_counter += 1
                if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at):
                    if git_commit_and_push(f"chore: checkpoint ped {horse_id}", allow_checkpoint_only=True):
                        checkpoint_counter = 0
                        last_checkpoint_push_at = time.time()
                        print(f"[PUSH] checkpoint ped {horse_id}", flush=True)
                    else:
                        print(f"[SKIP PUSH] no diff for ped {horse_id}", flush=True)
                elif args.git_push:
                    print(f"[SKIP PUSH] no diff for ped {horse_id}", flush=True)
        except Exception:
            error_text = traceback.format_exc(limit=3)
            summary["ped"]["failed"].append({"id": horse_id, "error": error_text})
            print(error_text, file=sys.stderr, flush=True)
        finally:
            write_summary(summary)
            time.sleep(args.sleep_seconds)

    if args.git_push and should_push_checkpoint(checkpoint_counter, last_checkpoint_push_at, force=True):
        last_ped_id = checkpoint.get("ped_last_id")
        if last_ped_id and git_commit_and_push(f"chore: checkpoint ped {last_ped_id}", allow_checkpoint_only=True):
            checkpoint_counter = 0
            last_checkpoint_push_at = time.time()
            print(f"[PUSH] checkpoint ped {last_ped_id}", flush=True)

    update_checkpoint(checkpoint, phase="done", latest_kaisai_date=latest_kaisai_date, item_id=None)
    if args.git_push:
        git_commit_and_push("chore: mark scrape resume checkpoint done", allow_checkpoint_only=True)

    write_summary(summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
