import os
import re
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from numpy import nan as NaN
from tqdm.auto import tqdm
from bs4 import BeautifulSoup, FeatureNotFound

from modules.constants import Master


def _make_soup(html: bytes) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        return BeautifulSoup(html, "html.parser")


def _is_raw_html_text(value: str) -> bool:
    s = value.lstrip()
    return s.startswith("<!DOCTYPE html") or s.startswith("<html") or s.startswith("<?xml")


def _load_html_bytes(html_source: Any) -> bytes:
    if isinstance(html_source, (bytes, bytearray)):
        return bytes(html_source)

    if isinstance(html_source, os.PathLike):
        with open(html_source, 'rb') as f:
            return f.read()

    if isinstance(html_source, str):
        if _is_raw_html_text(html_source):
            return html_source.encode('utf-8', errors='ignore')
        with open(html_source, 'rb') as f:
            return f.read()

    raise TypeError(
        f"Unsupported html source type: {type(html_source)!r}. Expected path, str HTML, or bytes."
    )


def _extract_numeric_id(
    html_source: Any,
    *,
    html: Optional[bytes] = None,
    digits: int,
    label: str,
) -> str:
    if isinstance(html_source, (str, os.PathLike)):
        source_text = str(html_source)
        if not _is_raw_html_text(source_text):
            match = re.search(rf"(\d{{{digits}}})\.bin$", str(Path(source_text).name))
            if match:
                return match.group(1)

    if html is not None:
        html_text = html.decode('utf-8', errors='ignore')
        patterns = [
            rf"/{label}/(\d{{{digits}}})",
            rf"{label}[_/=:-]?(\d{{{digits}}})",
            rf"(\d{{{digits}}})",
        ]
        for pattern in patterns:
            match = re.search(pattern, html_text)
            if match:
                return match.group(1)

    raise ValueError(
        f"Could not extract {label}_id from input. "
        f"Received type={type(html_source)!r}."
    )


def _concat_dict_frames(data: dict, label: str) -> pd.DataFrame:
    if not data:
        raise ValueError(f"No valid {label} were parsed from the provided html_path_list.")
    return pd.concat([data[key] for key in data])


def get_rawdata_results(html_path_list: list):
    """
    raceページのhtmlを受け取って、レース結果テーブルに変換する関数。
    html_path_list には、ファイルパスだけでなく raw HTML(bytes / str) も渡せる。
    """
    print('preparing raw results table')
    if not html_path_list:
        raise ValueError(
            "html_path_list is empty. "
            "scrape_html_race(..., skip=True) may have returned no usable files."
        )

    race_results = {}
    for html_source in tqdm(html_path_list):
        try:
            html = _load_html_bytes(html_source)
            df = pd.read_html(html)[0]
            soup = _make_soup(html)

            result_table = soup.find("table", attrs={"summary": "レース結果"})
            if result_table is None:
                raise ValueError("race result table not found")

            horse_id_list = []
            horse_a_list = result_table.find_all(
                "a", attrs={"href": re.compile("^/horse")}
            )
            for a in horse_a_list:
                horse_id = re.findall(r"\d+", a["href"])
                horse_id_list.append(horse_id[0])
            df["horse_id"] = horse_id_list

            jockey_id_list = []
            jockey_a_list = result_table.find_all(
                "a", attrs={"href": re.compile("^/jockey")}
            )
            for a in jockey_a_list:
                jockey_id = re.findall(r"jockey/result/recent/(\w*)", a["href"])
                jockey_id_list.append(jockey_id[0])
            df["jockey_id"] = jockey_id_list

            trainer_id_list = []
            trainer_a_list = result_table.find_all(
                "a", attrs={"href": re.compile("^/trainer")}
            )
            for a in trainer_a_list:
                trainer_id = re.findall(r"trainer/result/recent/(\w*)", a["href"])
                trainer_id_list.append(trainer_id[0])
            df["trainer_id"] = trainer_id_list

            owner_id_list = []
            owner_a_list = result_table.find_all(
                "a", attrs={"href": re.compile("^/owner")}
            )
            for a in owner_a_list:
                owner_id = re.findall(r"owner/result/recent/(\w*)", a["href"])
                owner_id_list.append(owner_id[0])
            df["owner_id"] = owner_id_list

            race_id = _extract_numeric_id(html_source, html=html, digits=12, label='race')
            df.index = [race_id] * len(df)
            race_results[race_id] = df
        except Exception as e:
            print('error at {}'.format(repr(html_source)[:160]))
            print(e)

    race_results_df = _concat_dict_frames(race_results, 'race result tables')
    race_results_df = race_results_df.rename(columns=lambda x: x.replace(' ', ''))
    return race_results_df


def get_rawdata_info(html_path_list: list):
    """
    raceページのhtmlを受け取って、レース情報テーブルに変換する関数。
    html_path_list には、ファイルパスだけでなく raw HTML(bytes / str) も渡せる。
    """
    print('preparing raw race_info table')
    if not html_path_list:
        raise ValueError(
            "html_path_list is empty. "
            "No race html files are available for get_rawdata_info()."
        )

    race_infos = {}
    for html_source in tqdm(html_path_list):
        try:
            html = _load_html_bytes(html_source)
            soup = _make_soup(html)

            texts = (
                soup.find("div", attrs={"class": "data_intro"}).find_all("p")[0].text
                + soup.find("div", attrs={"class": "data_intro"}).find_all("p")[1].text
            )
            info = re.findall(r'\w+', texts)
            df = pd.DataFrame()
            hurdle_race_flg = False
            for text in info:
                if text in ["芝", "ダート"]:
                    df["race_type"] = [text]
                if "障" in text:
                    df["race_type"] = ["障害"]
                    hurdle_race_flg = True
                if "0m" in text:
                    df["course_len"] = [int(re.findall(r"\d+", text)[-1])]
                if text in Master.GROUND_STATE_LIST:
                    df["ground_state"] = [text]
                if text in Master.WEATHER_LIST:
                    df["weather"] = [text]
                if "年" in text:
                    df["date"] = [text]
                if "右" in text:
                    df["around"] = [Master.AROUND_LIST[0]]
                if "左" in text:
                    df["around"] = [Master.AROUND_LIST[1]]
                if "直線" in text:
                    df["around"] = [Master.AROUND_LIST[2]]
                if "新馬" in text:
                    df["race_class"] = [Master.RACE_CLASS_LIST[0]]
                if "未勝利" in text:
                    df["race_class"] = [Master.RACE_CLASS_LIST[1]]
                if ("1勝クラス" in text) or ("500万下" in text):
                    df["race_class"] = [Master.RACE_CLASS_LIST[2]]
                if ("2勝クラス" in text) or ("1000万下" in text):
                    df["race_class"] = [Master.RACE_CLASS_LIST[3]]
                if ("3勝クラス" in text) or ("1600万下" in text):
                    df["race_class"] = [Master.RACE_CLASS_LIST[4]]
                if "オープン" in text:
                    df["race_class"] = [Master.RACE_CLASS_LIST[5]]

            grade_text = soup.find("div", attrs={"class": "data_intro"}).find_all("h1")[0].text
            if "G3" in grade_text:
                df["race_class"] = [Master.RACE_CLASS_LIST[6]] * len(df)
            elif "G2" in grade_text:
                df["race_class"] = [Master.RACE_CLASS_LIST[7]] * len(df)
            elif "G1" in grade_text:
                df["race_class"] = [Master.RACE_CLASS_LIST[8]] * len(df)

            if hurdle_race_flg:
                df["around"] = [Master.AROUND_LIST[3]]
                df["race_class"] = [Master.RACE_CLASS_LIST[9]]

            race_id = _extract_numeric_id(html_source, html=html, digits=12, label='race')
            df.index = [race_id] * len(df)
            race_infos[race_id] = df
        except Exception as e:
            print('error at {}'.format(repr(html_source)[:160]))
            print(e)

    race_infos_df = _concat_dict_frames(race_infos, 'race info tables')
    return race_infos_df


def get_rawdata_return(html_path_list: list):
    """
    raceページのhtmlを受け取って、払い戻しテーブルに変換する関数。
    html_path_list には、ファイルパスだけでなく raw HTML(bytes / str) も渡せる。
    """
    print('preparing raw return table')
    if not html_path_list:
        raise ValueError(
            "html_path_list is empty. "
            "No race html files are available for get_rawdata_return()."
        )

    race_return = {}
    for html_source in tqdm(html_path_list):
        try:
            html = _load_html_bytes(html_source)
            html = html.replace(b'<br />', b'br')
            dfs = pd.read_html(html)
            df = pd.concat([dfs[1], dfs[2]])
            race_id = _extract_numeric_id(html_source, html=html, digits=12, label='race')
            df.index = [race_id] * len(df)
            race_return[race_id] = df
        except Exception as e:
            print('error at {}'.format(repr(html_source)[:160]))
            print(e)

    race_return_df = _concat_dict_frames(race_return, 'race return tables')
    return race_return_df


def get_rawdata_horse_info(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の基本情報のDataFrameに変換する関数。
    html_path_list には、ファイルパスだけでなく raw HTML(bytes / str) も渡せる。
    """
    print('preparing raw horse_info table')
    horse_info = {}
    for html_source in tqdm(html_path_list):
        try:
            html = _load_html_bytes(html_source)
            df_info = pd.read_html(html)[1].set_index(0).T
            soup = _make_soup(html)

            try:
                trainer_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                    "a", attrs={"href": re.compile("^/trainer")}
                )
                trainer_id = re.findall(r"trainer/(\w*)", trainer_a_list[0]["href"])[0]
            except Exception:
                trainer_id = NaN
            df_info['trainer_id'] = trainer_id

            try:
                owner_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                    "a", attrs={"href": re.compile("^/owner")}
                )
                owner_id = re.findall(r"owner/(\w*)", owner_a_list[0]["href"])[0]
            except Exception:
                owner_id = NaN
            df_info['owner_id'] = owner_id

            try:
                breeder_a_list = soup.find("table", attrs={"summary": "のプロフィール"}).find_all(
                    "a", attrs={"href": re.compile("^/breeder")}
                )
                breeder_id = re.findall(r"breeder/(\w*)", breeder_a_list[0]["href"])[0]
            except Exception:
                breeder_id = NaN
            df_info['breeder_id'] = breeder_id

            horse_id = _extract_numeric_id(html_source, html=html, digits=10, label='horse')
            df_info.index = [horse_id] * len(df_info)
            horse_info[horse_id] = df_info
        except Exception as e:
            print('error at {}'.format(repr(html_source)[:160]))
            print(e)

    horse_info_df = _concat_dict_frames(horse_info, 'horse info tables')
    return horse_info_df


def get_rawdata_horse_results(html_path_list: list):
    """
    horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。
    html_path_list には、ファイルパスだけでなく raw HTML(bytes / str) も渡せる。
    """
    print('preparing raw horse_results table')
    horse_results = {}
    for html_source in tqdm(html_path_list):
        try:
            html = _load_html_bytes(html_source)
            df = pd.read_html(html)[3]
            if df.columns[0] == '受賞歴':
                df = pd.read_html(html)[4]
            if df.columns[0] == 0:
                print('horse_results empty case1 {}'.format(repr(html_source)[:160]))
                continue
            horse_id = _extract_numeric_id(html_source, html=html, digits=10, label='horse')
            df.index = [horse_id] * len(df)
            horse_results[horse_id] = df
        except IndexError:
            print('horse_results empty case2 {}'.format(repr(html_source)[:160]))
            continue
        except Exception as e:
            print('error at {}'.format(repr(html_source)[:160]))
            print(e)

    horse_results_df = _concat_dict_frames(horse_results, 'horse result tables')
    horse_results_df = horse_results_df.rename(columns=lambda x: x.replace(' ', ''))
    return horse_results_df


def get_rawdata_peds(html_path_list: list):
    """
    horse/pedページのhtmlを受け取って、血統のDataFrameに変換する関数。
    html_path_list には、ファイルパスだけでなく raw HTML(bytes / str) も渡せる。
    """
    print('preparing raw peds table')
    peds = {}
    for html_source in tqdm(html_path_list):
        try:
            html = _load_html_bytes(html_source)
            horse_id = _extract_numeric_id(html_source, html=html, digits=10, label='horse')
            soup = _make_soup(html)

            peds_id_list = []
            horse_a_list = soup.find("table", attrs={"summary": "5代血統表"}).find_all(
                "a", attrs={"href": re.compile(r"^/horse/\w{10}")}
            )
            for a in horse_a_list:
                work_peds_id = re.findall(r'horse\W(\w{10})', a["href"])[0]
                peds_id_list.append(work_peds_id)

            peds[horse_id] = peds_id_list
        except Exception as e:
            print('error at {}'.format(repr(html_source)[:160]))
            print(e)

    peds_df = pd.DataFrame.from_dict(peds, orient='index').add_prefix('peds_')
    return peds_df


def update_rawdata(filepath: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    filepathにrawテーブルのpickleファイルパスを指定し、new_dfに追加したいDataFrameを指定。
    元々のテーブルにnew_dfが追加されてpickleファイルが更新される。
    pickleファイルが存在しない場合は、filepathに新たに作成される。
    """
    if os.path.isfile(filepath):
        backupfilepath = filepath + '.bak'
        if new_df.empty:
            print('preparing update raw data empty')
        else:
            filedf = pd.read_pickle(filepath)
            filtered_old = filedf[~filedf.index.isin(new_df.index)]
            if os.path.isfile(backupfilepath):
                os.remove(backupfilepath)
            os.rename(filepath, backupfilepath)
            updated = pd.concat([filtered_old, new_df])
            updated.to_pickle(filepath)
    else:
        new_df.to_pickle(filepath)
