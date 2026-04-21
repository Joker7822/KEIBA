#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
netkeiba 全件スクレイピング用スクリプト

目的:
- race 一覧から race 詳細HTMLを保存
- horse 一覧から horse 詳細HTMLを保存
- horse_id から ped 詳細HTMLを保存
- 保存先を race / horse / ped フォルダで分離
- 再開可能 (途中停止しても続きから実行)
- 403 / 429 / タイムアウト時にリトライ
- requests で取得できない場合は Selenium にフォールバック可能

注意:
- サイトの利用規約・robots・アクセス負荷に注意してください。
- sleep を小さくしすぎるとブロックされやすくなります。
- 件数が非常に多いため、長時間実行前提です。

推奨:
    pip install requests beautifulsoup4 selenium tqdm

Selenium 備考:
- Chrome / Chromium がインストールされていれば Selenium Manager で動く構成です。
- requests で 403 が多い環境では --detail-driver selenium を使ってください。

実行例:
    python scrape_netkeiba_all.py \
      --horse-list-url "https://db.sp.netkeiba.com/?pid=horse_list&word=&match=partial_match&sire=&mare=&bms=&trainer=&owner=&breeder=&under_age=2&over_age=none&under_birthmonth=1&over_birthmonth=12&under_birthday=1&over_birthday=31&prize_min=&prize_max=&sort=prize&submit=" \
      --race-list-url "https://db.sp.netkeiba.com/?pid=race_list&word=&start_year=2010&start_mon=none&end_year=2026&end_mon=none&kyori_min=&kyori_max=&sort=date&submit=" \
      --out-dir data/netkeiba \
      --sleep 2.0 \
      --timeout 30 \
      --detail-driver auto
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None

# Selenium は optional
try:
    from selenium import webdriver
    from selenium.common.exceptions import WebDriverException, TimeoutException
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
except Exception:  # pragma: no cover
    webdriver = None
    WebDriverException = Exception
    TimeoutException = Exception
    ChromeOptions = None
    ChromeService = None
    By = None
    EC = None
    WebDriverWait = None


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

HORSE_DETAIL_RE = re.compile(r"https?://db\.netkeiba\.com/horse/(?!ped/)(\d+)/?$|^/horse/(?!ped/)(\d+)/?$")
HORSE_PED_RE = re.compile(r"https?://db\.netkeiba\.com/horse/ped/(\d+)/?$|^/horse/ped/(\d+)/?$")
RACE_DETAIL_RE = re.compile(r"https?://db\.netkeiba\.com/race/(\d+)/?$|^/race/(\d+)/?$")

BLOCK_PATTERNS = [
    "403 Forbidden",
    "Access Denied",
    "Too Many Requests",
    "attention required",
    "bot",
    "captcha",
]


@dataclass
class Config:
    horse_list_url: str
    race_list_url: str
    out_dir: Path
    sleep: float
    timeout: int
    list_driver: str
    detail_driver: str
    headless: bool
    max_list_pages: Optional[int]
    max_race_details: Optional[int]
    max_horse_details: Optional[int]
    max_ped_details: Optional[int]
    overwrite: bool
    skip_ped: bool
    no_resume: bool
    collect_only: bool
    skip_list_crawl: bool
    skip_race_details: bool
    skip_horse_details: bool


class ProgressStore:
    def __init__(self, root: Path, enable_resume: bool = True) -> None:
        self.root = root
        self.state_dir = root / "_state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.enable_resume = enable_resume

    def _path(self, name: str) -> Path:
        return self.state_dir / name

    def load_lines(self, name: str) -> Set[str]:
        if not self.enable_resume:
            return set()
        path = self._path(name)
        if not path.exists():
            return set()
        return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}

    def add_line(self, name: str, value: str) -> None:
        path = self._path(name)
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{value}\n")

    def append_jsonl(self, name: str, payload: dict) -> None:
        path = self._path(name)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


class BrowserFetcher:
    def __init__(self, headless: bool = True, timeout: int = 30) -> None:
        self.headless = headless
        self.timeout = timeout
        self.driver = None

    def start(self) -> None:
        if webdriver is None:
            raise RuntimeError("selenium が未インストールです。pip install selenium を実行してください。")
        options = ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1440,2200")
        options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
        options.add_argument("--lang=ja-JP")
        options.add_argument("--blink-settings=imagesEnabled=false")
        # Selenium Manager に任せる
        self.driver = webdriver.Chrome(options=options, service=ChromeService())
        self.driver.set_page_load_timeout(self.timeout)

    def get(self, url: str, wait_css: Optional[str] = None) -> str:
        if self.driver is None:
            self.start()
        assert self.driver is not None
        self.driver.get(url)
        if wait_css and WebDriverWait is not None:
            try:
                WebDriverWait(self.driver, self.timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )
            except TimeoutException:
                pass
        time.sleep(1.0)
        return self.driver.page_source

    def quit(self) -> None:
        if self.driver is not None:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None


class NetkeibaScraper:
    def __init__(self, config: Config) -> None:
        self.cfg = config
        self.out_race = self.cfg.out_dir / "race"
        self.out_horse = self.cfg.out_dir / "horse"
        self.out_ped = self.cfg.out_dir / "ped"
        self.out_race.mkdir(parents=True, exist_ok=True)
        self.out_horse.mkdir(parents=True, exist_ok=True)
        self.out_ped.mkdir(parents=True, exist_ok=True)
        self.progress = ProgressStore(self.cfg.out_dir, enable_resume=not self.cfg.no_resume)
        self.session = self._build_session()
        self.list_browser = None
        self.detail_browser = None

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.2,
            status_forcelist=[403, 408, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update(
            {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Referer": "https://db.netkeiba.com/",
            }
        )
        return s

    def _sleep(self) -> None:
        time.sleep(self.cfg.sleep + random.uniform(0.2, 1.0))

    def _needs_browser(self, html: str, status_code: int) -> bool:
        if status_code in {403, 429}:
            return True
        lower = html.lower()
        return any(p.lower() in lower for p in BLOCK_PATTERNS)

    def _get_browser(self, which: str) -> BrowserFetcher:
        if which == "list":
            if self.list_browser is None:
                self.list_browser = BrowserFetcher(headless=self.cfg.headless, timeout=self.cfg.timeout)
            return self.list_browser
        if self.detail_browser is None:
            self.detail_browser = BrowserFetcher(headless=self.cfg.headless, timeout=self.cfg.timeout)
        return self.detail_browser

    def fetch_html(self, url: str, mode: str, wait_css: Optional[str] = None) -> str:
        """
        mode: 'list' or 'detail'
        driver policy: requests / selenium / auto
        """
        driver_policy = self.cfg.list_driver if mode == "list" else self.cfg.detail_driver

        if driver_policy == "selenium":
            browser = self._get_browser(mode)
            return browser.get(url, wait_css=wait_css)

        # requests or auto
        last_error = None
        for attempt in range(1, 4):
            try:
                resp = self.session.get(url, timeout=self.cfg.timeout)
                html = resp.text or ""
                if driver_policy == "auto" and self._needs_browser(html, resp.status_code):
                    browser = self._get_browser(mode)
                    return browser.get(url, wait_css=wait_css)
                resp.raise_for_status()
                return html
            except Exception as exc:
                last_error = exc
                if driver_policy == "requests":
                    time.sleep(min(10, attempt * 2.0))
                else:
                    # auto: 失敗時も最後は Selenium へ寄せる
                    if attempt == 3:
                        try:
                            browser = self._get_browser(mode)
                            return browser.get(url, wait_css=wait_css)
                        except Exception as browser_exc:
                            last_error = browser_exc
                    else:
                        time.sleep(min(10, attempt * 2.0))
        raise RuntimeError(f"fetch failed: {url} :: {last_error}")

    @staticmethod
    def normalize_url(url: str, base: str) -> str:
        return urljoin(base, url)

    @staticmethod
    def extract_ids_from_html(html: str, base_url: str) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
        soup = BeautifulSoup(html, "html.parser")
        horse_ids: Set[str] = set()
        race_ids: Set[str] = set()
        ped_ids: Set[str] = set()
        list_links: Set[str] = set()

        parsed_base = urlparse(base_url)
        pid = "horse_list" if "horse_list" in base_url else "race_list" if "race_list" in base_url else None

        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            full = urljoin(base_url, href)

            m = HORSE_DETAIL_RE.search(href) or HORSE_DETAIL_RE.search(full)
            if m:
                horse_ids.add(next(g for g in m.groups() if g))
                continue

            m = RACE_DETAIL_RE.search(href) or RACE_DETAIL_RE.search(full)
            if m:
                race_ids.add(next(g for g in m.groups() if g))
                continue

            m = HORSE_PED_RE.search(href) or HORSE_PED_RE.search(full)
            if m:
                ped_ids.add(next(g for g in m.groups() if g))
                continue

            if pid and pid in full:
                parsed = urlparse(full)
                if parsed.netloc == parsed_base.netloc:
                    list_links.add(full)

        return horse_ids, race_ids, ped_ids, list_links

    @staticmethod
    def write_html(path: Path, html: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html, encoding="utf-8")

    def crawl_list_pages(self, start_url: str, entity: str) -> Set[str]:
        assert entity in {"race", "horse"}
        seen_pages = self.progress.load_lines(f"seen_{entity}_list_pages.txt")
        collected_ids = self.progress.load_lines(f"collected_{entity}_ids.txt")

        pending = [start_url]
        page_count = 0

        while pending:
            url = pending.pop(0)
            if url in seen_pages:
                continue
            if self.cfg.max_list_pages is not None and page_count >= self.cfg.max_list_pages:
                break

            print(f"[LIST:{entity}] {url}")
            try:
                html = self.fetch_html(url, mode="list")
            except Exception as exc:
                self.progress.append_jsonl(
                    f"failed_{entity}_list_pages.jsonl",
                    {"url": url, "error": str(exc), "at": int(time.time())},
                )
                continue

            horse_ids, race_ids, _, list_links = self.extract_ids_from_html(html, url)

            new_ids = race_ids if entity == "race" else horse_ids
            for entity_id in sorted(new_ids):
                if entity_id not in collected_ids:
                    collected_ids.add(entity_id)
                    self.progress.add_line(f"collected_{entity}_ids.txt", entity_id)

            seen_pages.add(url)
            self.progress.add_line(f"seen_{entity}_list_pages.txt", url)
            page_count += 1

            for link in sorted(list_links):
                if link not in seen_pages and link not in pending:
                    pending.append(link)

            self._sleep()

        print(f"[LIST:{entity}] collected={len(collected_ids)} pages={page_count}")
        return collected_ids

    def _detail_url(self, entity: str, entity_id: str) -> str:
        if entity == "race":
            return f"https://db.netkeiba.com/race/{entity_id}/"
        if entity == "horse":
            return f"https://db.netkeiba.com/horse/{entity_id}/"
        if entity == "ped":
            return f"https://db.netkeiba.com/horse/ped/{entity_id}/"
        raise ValueError(entity)

    def _detail_path(self, entity: str, entity_id: str) -> Path:
        folder = {"race": self.out_race, "horse": self.out_horse, "ped": self.out_ped}[entity]
        return folder / f"{entity_id}.html"

    def save_details(self, entity: str, ids: Iterable[str], max_items: Optional[int] = None) -> None:
        done = self.progress.load_lines(f"done_{entity}.txt")
        failed = self.progress.load_lines(f"failed_{entity}.txt")
        ids = list(dict.fromkeys(ids))

        pending_ids: list[str] = []
        skipped = 0
        for entity_id in ids:
            out_path = self._detail_path(entity, entity_id)
            if not self.cfg.overwrite and out_path.exists() and out_path.stat().st_size > 0:
                skipped += 1
                if entity_id not in done:
                    done.add(entity_id)
                    self.progress.add_line(f"done_{entity}.txt", entity_id)
                continue
            if entity_id in done and not self.cfg.overwrite:
                skipped += 1
                continue
            pending_ids.append(entity_id)

        if max_items is not None:
            pending_ids = pending_ids[:max_items]

        total_count = len(pending_ids)
        iterator = pending_ids
        if tqdm is not None and total_count > 1:
            iterator = tqdm(iterator, desc=f"scrape_{entity}")

        saved = 0
        for entity_id in iterator:
            out_path = self._detail_path(entity, entity_id)
            url = self._detail_url(entity, entity_id)
            try:
                html = self.fetch_html(url, mode="detail")
                self.write_html(out_path, html)
                done.add(entity_id)
                self.progress.add_line(f"done_{entity}.txt", entity_id)
                saved += 1
            except Exception as exc:
                if entity_id not in failed:
                    failed.add(entity_id)
                    self.progress.add_line(f"failed_{entity}.txt", entity_id)
                self.progress.append_jsonl(
                    f"failed_{entity}_detail.jsonl",
                    {"id": entity_id, "url": url, "error": str(exc), "at": int(time.time())},
                )
            self._sleep()

        print(f"[DETAIL:{entity}] saved={saved} skipped={skipped} total_target={total_count}")

    def run(self) -> None:
        try:
            if self.cfg.skip_list_crawl:
                race_ids = self.progress.load_lines("collected_race_ids.txt")
                horse_ids = self.progress.load_lines("collected_horse_ids.txt")
            else:
                race_ids = self.crawl_list_pages(self.cfg.race_list_url, "race")
                horse_ids = self.crawl_list_pages(self.cfg.horse_list_url, "horse")

            if self.cfg.collect_only:
                print(f"[COLLECT_ONLY] race_ids={len(race_ids)} horse_ids={len(horse_ids)}")
                return

            if not self.cfg.skip_race_details:
                self.save_details("race", sorted(race_ids), max_items=self.cfg.max_race_details)

            if not self.cfg.skip_horse_details:
                self.save_details("horse", sorted(horse_ids), max_items=self.cfg.max_horse_details)

            if not self.cfg.skip_ped:
                # ped は horse_id と同じ ID を利用
                ped_max = self.cfg.max_ped_details if self.cfg.max_ped_details is not None else self.cfg.max_horse_details
                self.save_details("ped", sorted(horse_ids), max_items=ped_max)
        finally:
            if self.list_browser is not None:
                self.list_browser.quit()
            if self.detail_browser is not None:
                self.detail_browser.quit()


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="netkeiba race / horse / ped スクレイパー")
    parser.add_argument("--horse-list-url", required=True, help="horse_list の開始URL")
    parser.add_argument("--race-list-url", required=True, help="race_list の開始URL")
    parser.add_argument("--out-dir", default="data/netkeiba", help="保存先ルート")
    parser.add_argument("--sleep", type=float, default=2.0, help="各アクセスの基本待機秒")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP / Browser timeout 秒")
    parser.add_argument(
        "--list-driver",
        choices=["auto", "requests", "selenium"],
        default="auto",
        help="一覧ページ取得方法",
    )
    parser.add_argument(
        "--detail-driver",
        choices=["auto", "requests", "selenium"],
        default="auto",
        help="詳細ページ取得方法",
    )
    parser.add_argument("--headless", action="store_true", default=True, help="headless Chrome を使う")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="headless 無効")
    parser.add_argument("--max-list-pages", type=int, default=None, help="一覧ページの最大巡回数")
    parser.add_argument("--max-race-details", type=int, default=None, help="race 詳細最大件数")
    parser.add_argument("--max-horse-details", type=int, default=None, help="horse 詳細最大件数")
    parser.add_argument("--max-ped-details", type=int, default=None, help="ped 詳細最大件数")
    parser.add_argument("--overwrite", action="store_true", help="既存HTMLを上書き")
    parser.add_argument("--skip-ped", action="store_true", help="ped 取得をスキップ")
    parser.add_argument("--no-resume", action="store_true", help="再開情報を使わない")
    parser.add_argument("--collect-only", action="store_true", help="一覧巡回のみ行い、詳細取得しない")
    parser.add_argument("--skip-list-crawl", action="store_true", help="一覧巡回をスキップし、既存stateの collected_*_ids を使う")
    parser.add_argument("--skip-race-details", action="store_true", help="race 詳細を取得しない")
    parser.add_argument("--skip-horse-details", action="store_true", help="horse 詳細を取得しない")

    args = parser.parse_args()

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
        max_race_details=args.max_race_details,
        max_horse_details=args.max_horse_details,
        max_ped_details=args.max_ped_details,
        overwrite=args.overwrite,
        skip_ped=args.skip_ped,
        no_resume=args.no_resume,
        collect_only=args.collect_only,
        skip_list_crawl=args.skip_list_crawl,
        skip_race_details=args.skip_race_details,
        skip_horse_details=args.skip_horse_details,
    )


def main() -> int:
    cfg = parse_args()
    scraper = NetkeibaScraper(cfg)
    scraper.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
