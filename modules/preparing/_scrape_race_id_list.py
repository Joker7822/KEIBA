import calendar
import datetime
import os
import re
import time
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from tqdm.auto import tqdm
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from modules.constants import UrlPaths
from ._prepare_chrome_driver import prepare_chrome_driver


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y%m%d",
    "%Y-%m",
    "%Y/%m",
)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def _parse_flexible_date(value: str, *, is_end: bool = False) -> datetime.date:
    """
    YYYY-MM-DD / YYYY/MM/DD / YYYYMMDD / YYYY-MM / YYYY/MM を受け付ける。
    月指定のみの場合、開始側は月初、終了側は月末で解釈する。
    """
    if not isinstance(value, str):
        raise TypeError("date value must be str")

    normalized = value.strip()
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.datetime.strptime(normalized, fmt)
            if fmt in {"%Y-%m", "%Y/%m"}:
                last_day = calendar.monthrange(parsed.year, parsed.month)[1]
                day = last_day if is_end else 1
                return datetime.date(parsed.year, parsed.month, day)
            return parsed.date()
        except ValueError:
            continue
    raise ValueError(
        f"Unsupported date format: {value}. "
        "Use YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD, YYYY-MM or YYYY/MM."
    )


def _month_start_iter(start_date: datetime.date, end_date: datetime.date) -> Iterable[Tuple[int, int]]:
    current = datetime.date(start_date.year, start_date.month, 1)
    last = datetime.date(end_date.year, end_date.month, 1)
    while current <= last:
        yield current.year, current.month
        if current.month == 12:
            current = datetime.date(current.year + 1, 1, 1)
        else:
            current = datetime.date(current.year, current.month + 1, 1)


def _normalize_kaisai_dates(kaisai_date_list: Iterable[str]) -> List[str]:
    return sorted({str(x) for x in kaisai_date_list if re.fullmatch(r"\d{8}", str(x))})


def _normalize_race_ids(race_id_list: Iterable[str]) -> List[str]:
    return sorted({str(x) for x in race_id_list if re.fullmatch(r"\d{12}", str(x))})


def _fetch_html(url: str, *, timeout: int = 30, max_attempt: int = 3, sleep_seconds: float = 1.0) -> bytes:
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


def scrape_kaisai_date(from_: str, to_: str, sleep_seconds: float = 1.0):
    """
    開始日 from_ から終了日 to_ まで（両端含む）のレース開催日一覧を返す。
    """
    start_date = _parse_flexible_date(from_, is_end=False)
    end_date = _parse_flexible_date(to_, is_end=True)
    if start_date > end_date:
        raise ValueError(f"from_ must be <= to_. from_={from_}, to_={to_}")

    print(f"getting race date from {start_date.isoformat()} to {end_date.isoformat()}")

    kaisai_date_list: List[str] = []
    for year, month in tqdm(list(_month_start_iter(start_date, end_date))):
        query = [
            "year=" + str(year),
            "month=" + str(month),
        ]
        url = UrlPaths.CALENDAR_URL + "?" + "&".join(query)
        try:
            html = _fetch_html(url, timeout=30, max_attempt=3, sleep_seconds=sleep_seconds)
        except Exception as exc:
            print(f"calendar fetch failed: {url} ({exc})")
            continue

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

        soup = BeautifulSoup(html, "html.parser")
        calendar_table = soup.find("table", class_="Calendar_Table")
        if calendar_table is None:
            print(f"calendar table not found: {url}")
            continue

        a_list = calendar_table.find_all("a")
        for a in a_list:
            href = a.get("href", "")
            matched = re.findall(r"(?<=kaisai_date=)\d+", href)
            if not matched:
                continue
            kaisai_date = matched[0]
            try:
                kaisai_day = datetime.datetime.strptime(kaisai_date, "%Y%m%d").date()
            except ValueError:
                continue
            if start_date <= kaisai_day <= end_date:
                kaisai_date_list.append(kaisai_date)

    return _normalize_kaisai_dates(kaisai_date_list)


def _save_race_ids_csv(path: str, race_id_list: Iterable[str]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    pd.DataFrame({"race_id": _normalize_race_ids(race_id_list)}).to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
    )


def _extract_race_ids_from_html(html: str) -> List[str]:
    patterns = [
        r"shutuba\.html\?race_id=(\d{12})",
        r"result\.html\?race_id=(\d{12})",
        r"/race/(\d{12})/?",
        r"race_id=(\d{12})",
    ]
    race_ids: List[str] = []
    for pattern in patterns:
        race_ids.extend(re.findall(pattern, html))
    return _normalize_race_ids(race_ids)


def _extract_race_ids_from_driver(driver) -> List[str]:
    from selenium.webdriver.common.by import By

    selectors = [
        ".RaceList_Box a",
        "[class*='RaceList'] a",
        "a[href*='shutuba.html?race_id=']",
        "a[href*='result.html?race_id=']",
        "a[href*='/race/']",
    ]

    hrefs: List[str] = []
    for selector in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for elem in elements:
                href = elem.get_attribute("href") or ""
                if href:
                    hrefs.append(href)
            if hrefs:
                break
        except Exception:
            continue

    if not hrefs:
        return _extract_race_ids_from_html(driver.page_source)

    race_ids: List[str] = []
    for href in hrefs:
        race_ids.extend(re.findall(r"race_id=(\d{12})", href))
        race_ids.extend(re.findall(r"/race/(\d{12})/?", href))

    if not race_ids:
        return _extract_race_ids_from_html(driver.page_source)

    return _normalize_race_ids(race_ids)


def _save_debug_html(kaisai_date: str, html: str) -> None:
    debug_dir = os.path.join("data", "tmp", "debug_race_list")
    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, f"{kaisai_date}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def scrape_race_id_list(
    kaisai_date_list: list,
    waiting_time: int = 10,
    save_csv_path: Optional[str] = None,
    continue_on_error: bool = True,
    dedupe: bool = True,
):
    """
    開催日を yyyymmdd の文字列リストで入れると、race_id 一覧を返す。
    """
    try:
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.common.exceptions import TimeoutException
    except ImportError as exc:
        raise ImportError(
            "selenium is required for scrape_race_id_list(). Install requirements.txt first."
        ) from exc

    normalized_kaisai_dates = _normalize_kaisai_dates(kaisai_date_list)
    race_id_list: List[str] = []
    driver = prepare_chrome_driver()
    driver.implicitly_wait(waiting_time)
    max_attempt = 2

    print("getting race_id_list")

    try:
        for kaisai_date in tqdm(normalized_kaisai_dates):
            try:
                query = [
                    "kaisai_date=" + str(kaisai_date)
                ]
                url = UrlPaths.RACE_LIST_URL + "?" + "&".join(query)
                print(f"scraping: {url}")

                found_ids: List[str] = []
                for attempt in range(1, max_attempt + 1):
                    driver.get(url)

                    try:
                        WebDriverWait(driver, waiting_time).until(
                            lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
                        )
                    except TimeoutException:
                        pass

                    time.sleep(2)

                    found_ids = _extract_race_ids_from_driver(driver)
                    if found_ids:
                        break

                    print(f"retry:{attempt}/{max_attempt} waiting more {waiting_time} seconds")
                    time.sleep(waiting_time)

                if not found_ids:
                    _save_debug_html(kaisai_date, driver.page_source)
                    raise RuntimeError(
                        f"race_id not found for kaisai_date={kaisai_date}. "
                        f"Saved page_source to data/tmp/debug_race_list/{kaisai_date}.html"
                    )

                race_id_list.extend(found_ids)

                if save_csv_path:
                    _save_race_ids_csv(save_csv_path, race_id_list)

            except Exception as e:
                print(f"kaisai_date {kaisai_date} failed: {e}")
                if not continue_on_error:
                    raise

    finally:
        try:
            driver.close()
        except Exception:
            pass
        try:
            driver.quit()
        except Exception:
            pass

    if dedupe:
        race_id_list = _normalize_race_ids(race_id_list)

    return race_id_list
