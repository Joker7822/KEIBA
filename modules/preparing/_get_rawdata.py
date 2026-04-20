import os
import re
import hashlib
from pathlib import Path

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


def _looks_like_html_text(value: str) -> bool:
    s = value.lstrip()
    return s.startswith("<!DOCTYPE html") or s.startswith("<html") or s.startswith("<?xml")


def _load_html_input(item, kind: str = "race"):
    """
    item can be:
      - file path (str / PathLike)
      - raw html bytes
      - raw html text
    returns: (html_bytes, source_name)
    """
    if isinstance(item, (bytes, bytearray)):
        html = bytes(item)
        return html, f"inline_{kind}_{hashlib.md5(html).hexdigest()[:12]}.html"

    if isinstance(item, str) and _looks_like_html_text(item):
        html = item.encode("utf-8", errors="ignore")
        return html, f"inline_{kind}_{hashlib.md5(html).hexdigest()[:12]}.html"

    if isinstance(item, os.PathLike):
        item = os.fspath(item)

    if isinstance(item, str):
        with open(item, 'rb') as f:
            return f.read(), item

    raise TypeError(f"Unsupported html input type: {type(item)}")


def _extract_numeric_id_from_source(source_name: str, html: bytes | None = None, kind: str = "race") -> str:
    name = str(source_name)

    # 1) file name/path
    m = re.search(r"(\d{10,12})\.bin$", Path(name).name)
    if m:
        return m.group(1)

    # 2) URL / embedded page content
    if html is not None:
        text = html.decode('utf-8', errors='ignore')

        if kind == "race":
            patterns = [
                r"/race/(\d{12})",
                r"race_id[=:\"']+(\d{12})",
                r"race\W(\d{12})",
            ]
        else:
            patterns = [
                r"/horse/(\d{10})",
                r"horse_id[=:\"']+(\d{10})",
                r"horse\W(\d{10})",
            ]

        for pat in patterns:
            m = re.search(pat, text)
            if m:
                return m.group(1)

    raise ValueError(f"Could not extract {kind}_id from source: {source_name}")


def _concat_dict_frames(data: dict, label: str) -> pd.DataFrame:
    if not data:
        raise ValueError(f"No valid {label} were parsed from the provided html inputs.")
    return pd.concat([data[key] for key in data])


def get_rawdata_results(html_path_list: list):
    """raceページのhtmlを受け取って、レース結果テーブルに変換する関数。"""
    print('preparing raw results table')
    if not html_path_list:
        raise ValueError('html_path_list is empty.')

    race_results = {}
    for item in tqdm(html_path_list):
        try:
            html, source_name = _load_html_input(item, kind="race")
            df = pd.read_html(html)[0]
            soup = _make_soup(html)

            result_table = soup.find("table", attrs={"summary": "レース結果"})
            if result_table is None:
                raise ValueError("race result table not found")

            horse_id_list = []
            for a in result_table.find_all("a", attrs={"href": re.compile(r"^/horse")}):
                horse_id = re.findall(r"\d+", a["href"])
                if horse_id:
                    horse_id_list.append(horse_id[0])
            if horse_id_list:
                df["horse_id"] = horse_id_list[:len(df)]

            jockey_id_list = []
            for a in result_table.find_all("a", attrs={"href": re.compile(r"^/jockey")}):
                jockey_id = re.findall(r"jockey/result/recent/(\w*)", a["href"])
                if jockey_id:
                    jockey_id_list.append(jockey_id[0])
            if jockey_id_list:
                df["jockey_id"] = jockey_id_list[:len(df)]

            trainer_id_list = []
            for a in result_table.find_all("a", attrs={"href": re.compile(r"^/trainer")}):
                trainer_id = re.findall(r"trainer/result/recent/(\w*)", a["href"])
                if trainer_id:
                    trainer_id_list.append(trainer_id[0])
            if trainer_id_list:
                df["trainer_id"] = trainer_id_list[:len(df)]

            owner_id_list = []
            for a in result_table.find_all("a", attrs={"href": re.compile(r"^/owner")}):
                owner_id = re.findall(r"owner/result/recent/(\w*)", a["href"])
                if owner_id:
                    owner_id_list.append(owner_id[0])
            if owner_id_list:
                df["owner_id"] = owner_id_list[:len(df)]

            race_id = _extract_numeric_id_from_source(source_name, html, kind="race")
            df.index = [race_id] * len(df)
            race_results[race_id] = df
        except Exception as e:
            print(f'error at {repr(item)[:160]}')
            print(e)

    race_results_df = _concat_dict_frames(race_results, 'race result tables')
    race_results_df = race_results_df.rename(columns=lambda x: x.replace(' ', '') if isinstance(x, str) else x)
    return race_results_df


def get_rawdata_info(html_path_list: list):
    """raceページのhtmlを受け取って、レース情報テーブルに変換する関数。"""
    print('preparing raw race_info table')
    if not html_path_list:
        raise ValueError('html_path_list is empty.')

    race_infos = {}
    for item in tqdm(html_path_list):
        try:
            html, source_name = _load_html_input(item, kind="race")
            soup = _make_soup(html)

            data_intro = soup.find("div", attrs={"class": "data_intro"})
            if data_intro is None:
                raise ValueError('data_intro block not found')
            intro_ps = data_intro.find_all("p")
            if len(intro_ps) < 2:
                raise ValueError('data_intro paragraphs are missing')

            texts = intro_ps[0].text + intro_ps[1].text
            info = re.findall(r'\w+', texts)
            df = pd.DataFrame(index=[0])
            hurdle_race_flg = False
            for text in info:
                if text in ["芝", "ダート"]:
                    df["race_type"] = text
                if "障" in text:
                    df["race_type"] = "障害"
                    hurdle_race_flg = True
                if "0m" in text:
                    df["course_len"] = int(re.findall(r"\d+", text)[-1])
                if text in Master.GROUND_STATE_LIST:
                    df["ground_state"] = text
                if text in Master.WEATHER_LIST:
                    df["weather"] = text
                if "年" in text:
                    df["date"] = text
                if "右" in text:
                    df["around"] = Master.AROUND_LIST[0]
                if "左" in text:
                    df["around"] = Master.AROUND_LIST[1]
                if "直線" in text:
                    df["around"] = Master.AROUND_LIST[2]
                if "新馬" in text:
                    df["race_class"] = Master.RACE_CLASS_LIST[0]
                if "未勝利" in text:
                    df["race_class"] = Master.RACE_CLASS_LIST[1]
                if ("1勝クラス" in text) or ("500万下" in text):
                    df["race_class"] = Master.RACE_CLASS_LIST[2]
                if ("2勝クラス" in text) or ("1000万下" in text):
                    df["race_class"] = Master.RACE_CLASS_LIST[3]
                if ("3勝クラス" in text) or ("1600万下" in text):
                    df["race_class"] = Master.RACE_CLASS_LIST[4]
                if "オープン" in text:
                    df["race_class"] = Master.RACE_CLASS_LIST[5]

            grade_text = data_intro.find_all("h1")[0].text if data_intro.find_all("h1") else ""
            if "G3" in grade_text:
                df["race_class"] = Master.RACE_CLASS_LIST[6]
            elif "G2" in grade_text:
                df["race_class"] = Master.RACE_CLASS_LIST[7]
            elif "G1" in grade_text:
                df["race_class"] = Master.RACE_CLASS_LIST[8]

            if hurdle_race_flg:
                df["around"] = Master.AROUND_LIST[3]
                df["race_class"] = Master.RACE_CLASS_LIST[9]

            race_id = _extract_numeric_id_from_source(source_name, html, kind="race")
            df.index = [race_id] * len(df)
            race_infos[race_id] = df
        except Exception as e:
            print(f'error at {repr(item)[:160]}')
            print(e)

    return _concat_dict_frames(race_infos, 'race info tables')


def get_rawdata_return(html_path_list: list):
    """raceページのhtmlを受け取って、払い戻しテーブルに変換する関数。"""
    print('preparing raw return table')
    if not html_path_list:
        raise ValueError('html_path_list is empty.')

    race_return = {}
    for item in tqdm(html_path_list):
        try:
            html, source_name = _load_html_input(item, kind="race")
            html = html.replace(b'<br />', b'br')
            dfs = pd.read_html(html)
            if len(dfs) < 3:
                raise ValueError('return tables are missing')
            df = pd.concat([dfs[1], dfs[2]])
            race_id = _extract_numeric_id_from_source(source_name, html, kind="race")
            df.index = [race_id] * len(df)
            race_return[race_id] = df
        except Exception as e:
            print(f'error at {repr(item)[:160]}')
            print(e)

    return _concat_dict_frames(race_return, 'return tables')


def get_rawdata_horse_info(html_path_list: list):
    """horseページのhtmlを受け取って、馬の基本情報のDataFrameに変換する関数。"""
    print('preparing raw horse_info table')
    horse_info = {}
    for item in tqdm(html_path_list):
        try:
            html, source_name = _load_html_input(item, kind="horse")
            df_info = pd.read_html(html)[1].set_index(0).T
            soup = _make_soup(html)
            profile_table = soup.find("table", attrs={"summary": "のプロフィール"})

            try:
                trainer_a_list = profile_table.find_all("a", attrs={"href": re.compile(r"^/trainer")}) if profile_table else []
                trainer_id = re.findall(r"trainer/(\w*)", trainer_a_list[0]["href"])[0]
            except Exception:
                trainer_id = NaN
            df_info['trainer_id'] = trainer_id

            try:
                owner_a_list = profile_table.find_all("a", attrs={"href": re.compile(r"^/owner")}) if profile_table else []
                owner_id = re.findall(r"owner/(\w*)", owner_a_list[0]["href"])[0]
            except Exception:
                owner_id = NaN
            df_info['owner_id'] = owner_id

            try:
                breeder_a_list = profile_table.find_all("a", attrs={"href": re.compile(r"^/breeder")}) if profile_table else []
                breeder_id = re.findall(r"breeder/(\w*)", breeder_a_list[0]["href"])[0]
            except Exception:
                breeder_id = NaN
            df_info['breeder_id'] = breeder_id

            horse_id = _extract_numeric_id_from_source(source_name, html, kind="horse")
            df_info.index = [horse_id] * len(df_info)
            horse_info[horse_id] = df_info
        except Exception as e:
            print(f'error at {repr(item)[:160]}')
            print(e)

    return _concat_dict_frames(horse_info, 'horse info tables')


def get_rawdata_horse_results(html_path_list: list):
    """horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。"""
    print('preparing raw horse_results table')
    horse_results = {}
    for item in tqdm(html_path_list):
        try:
            html, source_name = _load_html_input(item, kind="horse")
            df = pd.read_html(html)[3]
            if df.columns[0] == '受賞歴':
                df = pd.read_html(html)[4]
            if df.columns[0] == 0:
                print(f'horse_results empty case1 {repr(item)[:160]}')
                continue
            horse_id = _extract_numeric_id_from_source(source_name, html, kind="horse")
            df.index = [horse_id] * len(df)
            horse_results[horse_id] = df
        except IndexError:
            print(f'horse_results empty case2 {repr(item)[:160]}')
            continue
        except Exception as e:
            print(f'error at {repr(item)[:160]}')
            print(e)

    horse_results_df = _concat_dict_frames(horse_results, 'horse result tables')
    horse_results_df = horse_results_df.rename(columns=lambda x: x.replace(' ', '') if isinstance(x, str) else x)
    return horse_results_df


def get_rawdata_peds(html_path_list: list):
    """horse/pedページのhtmlを受け取って、血統のDataFrameに変換する関数。"""
    print('preparing raw peds table')
    peds = {}
    for item in tqdm(html_path_list):
        try:
            html, source_name = _load_html_input(item, kind="horse")
            horse_id = _extract_numeric_id_from_source(source_name, html, kind="horse")
            soup = _make_soup(html)
            table = soup.find("table", attrs={"summary": "5代血統表"})
            if table is None:
                raise ValueError('ped table not found')
            peds_id_list = []
            for a in table.find_all("a", attrs={"href": re.compile(r"^/horse/\w{10}")}):
                work_peds_id = re.findall(r'horse\W(\w{10})', a["href"])[0]
                peds_id_list.append(work_peds_id)
            peds[horse_id] = peds_id_list
        except Exception as e:
            print(f'error at {repr(item)[:160]}')
            print(e)

    return pd.DataFrame.from_dict(peds, orient='index').add_prefix('peds_')


def update_rawdata(filepath: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """filepathにrawテーブルのpickleファイルパスを指定し、new_dfに追加したいDataFrameを指定。"""
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
