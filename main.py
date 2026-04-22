#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
main.py

目的:
- scrape_netkeiba_all.py の実行入口を分離する
- race / horse / ped / all を切り替えて実行できるようにする
- GitHub Actions / ローカル実行の両方で扱いやすくする

更新日: 2026-04-22 (JST)

実行例:
    python main.py --target race --max-list-pages 5 --detail-count 1
    python main.py --target horse --skip-list-crawl --detail-count 1
    python main.py --target ped --skip-list-crawl --detail-count 1
    python main.py --target all --max-list-pages 3 --detail-count 10
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Optional

from scrape_netkeiba_all import Config, NetkeibaScraper

DEFAULT_HORSE_LIST_URL = (
    "https://db.sp.netkeiba.com/?pid=horse_list&word=&match=partial_match"
    "&sire=&mare=&bms=&trainer=&owner=&breeder=&under_age=2&over_age=none"
    "&under_birthmonth=1&over_birthmonth=12&under_birthday=1&over_birthday=31"
    "&prize_min=&prize_max=&sort=prize&submit="
)

DEFAULT_RACE_LIST_URL = (
    "https://db.sp.netkeiba.com/?pid=race_list&word=&start_year=2010&start_mon=none"
    "&end_year=2026&end_mon=none&kyori_min=&kyori_max=&sort=date&submit="
)


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value else default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="netkeiba 詳細スクレイピング用 main エントリ")
    parser.add_argument(
        "--target",
        choices=["all", "race", "horse", "ped"],
        default=os.getenv("TARGET", "all"),
        help="取得対象",
    )
    parser.add_argument(
        "--horse-list-url",
        default=env_or_default("HORSE_LIST_URL", DEFAULT_HORSE_LIST_URL),
        help="horse 一覧開始URL",
    )
    parser.add_argument(
        "--race-list-url",
        default=env_or_default("RACE_LIST_URL", DEFAULT_RACE_LIST_URL),
        help="race 一覧開始URL",
    )
    parser.add_argument(
        "--out-dir",
        default=os.getenv("OUT_DIR", "data/netkeiba"),
        help="保存先ルート",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=float(os.getenv("SLEEP", "2.0")),
        help="各アクセス間の待機秒",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("TIMEOUT", "30")),
        help="タイムアウト秒",
    )
    parser.add_argument(
        "--list-driver",
        choices=["auto", "requests", "selenium"],
        default=os.getenv("LIST_DRIVER", "auto"),
        help="一覧ページ取得方法",
    )
    parser.add_argument(
        "--detail-driver",
        choices=["auto", "requests", "selenium"],
        default=os.getenv("DETAIL_DRIVER", "auto"),
        help="詳細ページ取得方法",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=os.getenv("HEADLESS", "1") != "0",
        help="headless Chrome を使う",
    )
    parser.add_argument(
        "--no-headless",
        action="store_false",
        dest="headless",
        help="headless Chrome を使わない",
    )
    parser.add_argument(
        "--max-list-pages",
        type=int,
        default=(int(os.getenv("MAX_LIST_PAGES")) if os.getenv("MAX_LIST_PAGES") else None),
        help="1回で巡回する一覧ページ最大数",
    )
    parser.add_argument(
        "--detail-count",
        type=int,
        default=(int(os.getenv("DETAIL_COUNT")) if os.getenv("DETAIL_COUNT") else 1),
        help="1回で取得する詳細ページ最大件数",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=os.getenv("OVERWRITE", "0") == "1",
        help="既存HTMLを上書き取得する",
    )
    parser.add_argument(
        "--skip-list-crawl",
        action="store_true",
        default=os.getenv("SKIP_LIST_CRAWL", "0") == "1",
        help="一覧巡回を行わず state の collected_*_ids を使う",
    )
    parser.add_argument(
        "--collect-only",
        action="store_true",
        default=os.getenv("COLLECT_ONLY", "0") == "1",
        help="一覧巡回のみ行い、詳細取得しない",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        default=os.getenv("NO_RESUME", "0") == "1",
        help="再開情報を使わない",
    )
    parser.add_argument(
        "--summary-file",
        default=os.getenv("SUMMARY_FILE", "data/netkeiba/_state/run_summary.json"),
        help="実行結果JSONの保存先",
    )
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> Config:
    target = args.target

    skip_race_details = target in {"horse", "ped"}
    skip_horse_details = target in {"race", "ped"}
    skip_ped = target in {"race", "horse"}

    max_race_details: Optional[int] = args.detail_count if target in {"all", "race"} else 0
    max_horse_details: Optional[int] = args.detail_count if target in {"all", "horse", "ped"} else 0
    max_ped_details: Optional[int] = args.detail_count if target in {"all", "ped"} else 0

    return Config(
        horse_list_url=args.horse_list_url,
        race_list_url=args.race_list_url,
        out_dir=Path(args.out_dir),
        sleep=args.sleep,
        timeout=args.timeout,
        list_driver=args.list_driver,
        detail_driver=args.detail_driver,
        headless=args.headless,
        max_list_pages=args.max_list_pages,
        max_race_details=max_race_details,
        max_horse_details=max_horse_details,
        max_ped_details=max_ped_details,
        overwrite=args.overwrite,
        skip_ped=skip_ped,
        no_resume=args.no_resume,
        collect_only=args.collect_only,
        skip_list_crawl=args.skip_list_crawl,
        skip_race_details=skip_race_details,
        skip_horse_details=skip_horse_details,
        summary_file=Path(args.summary_file) if args.summary_file else None,
    )


def print_run_header(args: argparse.Namespace, cfg: Config) -> None:
    print("=" * 80)
    print("netkeiba scraper main")
    print(f"target         : {args.target}")
    print(f"horse_list_url : {cfg.horse_list_url}")
    print(f"race_list_url  : {cfg.race_list_url}")
    print(f"out_dir        : {cfg.out_dir}")
    print(f"list_driver    : {cfg.list_driver}")
    print(f"detail_driver  : {cfg.detail_driver}")
    print(f"max_list_pages : {cfg.max_list_pages}")
    print(f"detail_count   : race={cfg.max_race_details}, horse={cfg.max_horse_details}, ped={cfg.max_ped_details}")
    print(f"skip_list_crawl: {cfg.skip_list_crawl}")
    print(f"collect_only   : {cfg.collect_only}")
    print("=" * 80)


def main() -> int:
    args = parse_args()
    cfg = build_config(args)
    print_run_header(args, cfg)

    scraper = NetkeibaScraper(cfg)
    summary = scraper.run()

    if cfg.summary_file is not None:
        cfg.summary_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.summary_file.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[SUMMARY] {cfg.summary_file}")

    details = summary.get("details", {})
    race_saved = int(details.get("race", {}).get("saved", 0))
    horse_saved = int(details.get("horse", {}).get("saved", 0))
    ped_saved = int(details.get("ped", {}).get("saved", 0))

    print(
        "[RESULT] "
        f"race_saved={race_saved} "
        f"horse_saved={horse_saved} "
        f"ped_saved={ped_saved} "
        f"race_ids={summary.get('race_ids', 0)} "
        f"horse_ids={summary.get('horse_ids', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
