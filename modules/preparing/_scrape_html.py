import datetime
import re
import pandas as pd
import time
import os
from typing import Optional

from tqdm.auto import tqdm
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup, FeatureNotFound

from modules.constants import UrlPaths, LocalPaths


_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}


def _ensure_dirs() -> None:
    os.makedirs(LocalPaths.HTML_RACE_DIR, exist_ok=True)
    os.makedirs(LocalPaths.HTML_HORSE_DIR, exist_ok=True)
    os.makedirs(LocalPaths.HTML_PED_DIR, exist_ok=True)
    os.makedirs(LocalPaths.MASTER_DIR, exist_ok=True)


def _fetch_html(
    url: str,
    *,
    timeout: int = 30,
    max_attempt: int = 3,
    sleep_seconds: float = 1.0,
) -> bytes:
    last_error: Optional[Exception] = None

    for attempt in range(1, max_attempt + 1):
        try:
            request = Request(url, headers=_DEFAULT_HEADERS)
            with urlopen(request, timeout=timeout) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            if attempt >= max_attempt:
                break
            wait = max(0.5, sleep_seconds) * attempt
            print(f"fetch retry {attempt}/{max_attempt} for {url}: {exc}")
            time.sleep(wait)

    assert last_error is not None
    raise last_error


def _make_soup(html: bytes) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        print("lxml is not available. Falling back to html.parser")
        return BeautifulSoup(html, "html.parser")


def _normalize_id(raw_id: object) -> str:
    value = str(raw_id).strip()
    if value.endswith(".0"):
        value = value[:-2]
    return value


def _is_valid_race_id(race_id: str) -> bool:
    return bool(re.fullmatch(r"\d{12}", str(race_id)))


def _is_valid_horse_id(horse_id: str) -> bool:
    return bool(re.fullmatch(r"\d{10}", str(horse_id)))


def scrape_html_race(race_id_list: list, skip: bool = True):
    """
    netkeiba.comのraceページのhtmlをスクレイピングしてdata/html/raceに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合は再取得せず、その既存ファイルも返り値に含める。
    skip=Falseにすると再取得して上書きする。
    返り値：利用可能なhtmlのファイルパス（既存 + 新規取得）
    """
    _ensure_dirs()
    html_path_list = []

    for race_id in tqdm(race_id_list):
        race_id = _normalize_id(race_id)

        if not _is_valid_race_id(race_id):
            print(f"race_id {race_id} skipped. Invalid format.")
            continue

        filename = os.path.join(LocalPaths.HTML_RACE_DIR, race_id + ".bin")

        if skip and os.path.isfile(filename):
            html_path_list.append(filename)
            continue

        url = UrlPaths.RACE_URL + race_id

        try:
            time.sleep(1)
            html = _fetch_html(url, timeout=30, max_attempt=3, sleep_seconds=1.0)
        except HTTPError as exc:
            print(f"race_id {race_id} skipped. HTTPError: {exc.code} {exc.reason}")
            continue
        except URLError as exc:
            print(f"race_id {race_id} skipped. URLError: {exc}")
            continue
        except Exception as exc:
            print(f"race_id {race_id} skipped. Unexpected error: {exc}")
            continue

        soup = _make_soup(html)
        data_intro_exists = bool(soup.find("div", attrs={"class": "data_intro"}))

        if not data_intro_exists:
            print(f"race_id {race_id} skipped. This page is not valid.")
            continue

        with open(filename, "wb") as f:
            f.write(html)

        html_path_list.append(filename)

    return html_path_list


def scrape_html_horse(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    _ensure_dirs()
    updated_html_path_list = []

    for horse_id in tqdm(horse_id_list):
        horse_id = _normalize_id(horse_id)

        if not _is_valid_horse_id(horse_id):
            print(f"horse_id {horse_id} skipped. Invalid format.")
            continue

        filename = os.path.join(LocalPaths.HTML_HORSE_DIR, horse_id + ".bin")

        if skip and os.path.isfile(filename):
            print(f"horse_id {horse_id} skipped")
            continue

        url = UrlPaths.HORSE_URL + horse_id

        try:
            time.sleep(1)
            html = _fetch_html(url, timeout=30, max_attempt=3, sleep_seconds=1.0)
        except HTTPError as exc:
            print(f"horse_id {horse_id} skipped. HTTPError: {exc.code} {exc.reason}")
            continue
        except URLError as exc:
            print(f"horse_id {horse_id} skipped. URLError: {exc}")
            continue
        except Exception as exc:
            print(f"horse_id {horse_id} skipped. Unexpected error: {exc}")
            continue

        with open(filename, "wb") as f:
            f.write(html)

        updated_html_path_list.append(filename)

    return updated_html_path_list


def scrape_html_ped(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorse/pedページのhtmlをスクレイピングしてdata/html/pedに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    """
    _ensure_dirs()
    updated_html_path_list = []

    for horse_id in tqdm(horse_id_list):
        horse_id = _normalize_id(horse_id)

        if not _is_valid_horse_id(horse_id):
            print(f"horse_id {horse_id} skipped. Invalid format.")
            continue

        filename = os.path.join(LocalPaths.HTML_PED_DIR, horse_id + ".bin")

        if skip and os.path.isfile(filename):
            print(f"horse_id {horse_id} skipped")
            continue

        url = UrlPaths.PED_URL + horse_id

        try:
            time.sleep(1)
            html = _fetch_html(url, timeout=30, max_attempt=3, sleep_seconds=1.0)
        except HTTPError as exc:
            print(f"horse_id {horse_id} skipped. HTTPError: {exc.code} {exc.reason}")
            continue
        except URLError as exc:
            print(f"horse_id {horse_id} skipped. URLError: {exc}")
            continue
        except Exception as exc:
            print(f"horse_id {horse_id} skipped. Unexpected error: {exc}")
            continue

        with open(filename, "wb") as f:
            f.write(html)

        updated_html_path_list.append(filename)

    return updated_html_path_list


def scrape_html_horse_with_master(horse_id_list: list, skip: bool = True):
    """
    netkeiba.comのhorseページのhtmlをスクレイピングしてdata/html/horseに保存する関数。
    skip=Trueにすると、すでにhtmlが存在する場合はスキップされ、Falseにすると上書きされる。
    返り値：新しくスクレイピングしたhtmlのファイルパス
    また、horse_idごとに、最後にスクレイピングした日付を記録し、data/master/horse_results_updated_at.csvに保存する。
    """
    _ensure_dirs()

    print("scraping")
    updated_html_path_list = scrape_html_horse(horse_id_list, skip)

    horse_id_list = [
        re.findall(r"horse\W(\d+).bin", html_path)[0]
        for html_path in updated_html_path_list
    ]
    horse_id_df = pd.DataFrame({"horse_id": horse_id_list})

    print("updating master")
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not os.path.isfile(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH):
        pd.DataFrame(columns=["horse_id", "updated_at"]).to_csv(
            LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH,
            index=None,
        )

    master = pd.read_csv(LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH, dtype=object)
    new_master = master.merge(horse_id_df, on="horse_id", how="outer")
    new_master.loc[new_master["horse_id"].isin(horse_id_list), "updated_at"] = now
    new_master[["horse_id", "updated_at"]].to_csv(
        LocalPaths.MASTER_RAW_HORSE_RESULTS_PATH,
        index=None,
    )

    return updated_html_path_list
