import argparse
import glob
import os
import traceback
from typing import Iterable, List, Optional, Tuple

import pandas as pd

from modules import preparing
from modules.constants import LocalPaths


DEFAULT_START = '2010/01/01'
DEFAULT_END = '2026/04/19'


def _dedupe_keep_order(items: Iterable[str]) -> List[str]:
    seen = set()
    result: List[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _candidate_race_html_dirs(downloaded_paths: Optional[List[str]] = None) -> List[str]:
    candidates: List[str] = []

    if downloaded_paths:
        for path in downloaded_paths:
            if path:
                candidates.append(os.path.dirname(path))

    explicit = getattr(LocalPaths, 'HTML_RACE_DIR', None)
    if explicit:
        candidates.append(explicit)

    base_dir = getattr(LocalPaths, 'BASE_DIR', os.getcwd())
    data_dir = getattr(LocalPaths, 'DATA_DIR', os.path.join(base_dir, 'data'))

    candidates.extend([
        os.path.join(data_dir, 'html', 'race'),
        os.path.join(data_dir, 'html_race'),
        os.path.join(data_dir, 'race'),
        os.path.join(base_dir, 'data', 'html', 'race'),
        os.path.join(base_dir, 'data', 'html_race'),
        os.path.join(base_dir, 'html', 'race'),
        os.path.join(base_dir, 'race'),
    ])

    return _dedupe_keep_order([os.path.abspath(path) for path in candidates if path])


def _resolve_race_html_dir(downloaded_paths: Optional[List[str]] = None) -> str:
    candidates = _candidate_race_html_dirs(downloaded_paths)
    for path in candidates:
        if os.path.isdir(path):
            return path
    return candidates[0] if candidates else os.path.abspath('data/html/race')


def _resolve_local_race_html_paths(
    race_id_list: List[str],
    downloaded_paths: Optional[List[str]] = None,
) -> Tuple[List[str], List[str], str]:
    race_html_dir = _resolve_race_html_dir(downloaded_paths)

    resolved: List[str] = []
    missing: List[str] = []

    for race_id in race_id_list:
        patterns = [
            os.path.join(race_html_dir, f'{race_id}*.bin'),
            os.path.join(race_html_dir, f'{race_id}*.html'),
            os.path.join(race_html_dir, f'*{race_id}*.bin'),
            os.path.join(race_html_dir, f'*{race_id}*.html'),
        ]
        matches: List[str] = []
        for pattern in patterns:
            matches.extend(glob.glob(pattern))
        matches = [path for path in _dedupe_keep_order(sorted(matches)) if os.path.isfile(path)]

        if matches:
            resolved.append(matches[0])
        else:
            missing.append(race_id)

    return resolved, missing, race_html_dir


def _print_html_debug_info(html_files: List[str], head_bytes: int = 500) -> None:
    print(f'html_files count: {len(html_files)}')
    for idx, path in enumerate(html_files[:5], start=1):
        exists = os.path.exists(path)
        size = os.path.getsize(path) if exists else 'missing'
        print(f'[{idx}] path={path}')
        print(f'    exists={exists} size={size}')

    if not html_files:
        print('html_files is empty.')
        return

    first_file = html_files[0]
    print(f'first html file: {first_file}')
    if not os.path.exists(first_file):
        print('first html file does not exist.')
        return

    with open(first_file, 'rb') as fp:
        head = fp.read(max(1, head_bytes))

    print(f'head[:{head_bytes}] = {head!r}')



def _run_single_file_parse_check(html_files: List[str], limit: int) -> None:
    if not html_files:
        print('parse check skipped: html_files is empty.')
        return

    target_files = html_files[: max(1, limit)]
    print(f'parse check target files: {len(target_files)}')

    for idx, path in enumerate(target_files, start=1):
        print(f'\n[{idx}/{len(target_files)}] parse check: {path}')

        try:
            results_df = preparing.get_rawdata_results([path])
            print(f'  results: OK shape={getattr(results_df, "shape", None)}')
        except Exception as e:
            print(f'  results: NG {e!r}')
            traceback.print_exc()

        try:
            race_info_df = preparing.get_rawdata_info([path])
            print(f'  info   : OK shape={getattr(race_info_df, "shape", None)}')
        except Exception as e:
            print(f'  info   : NG {e!r}')
            traceback.print_exc()

        try:
            return_df = preparing.get_rawdata_return([path])
            print(f'  return : OK shape={getattr(return_df, "shape", None)}')
        except Exception as e:
            print(f'  return : NG {e!r}')
            traceback.print_exc()



def main() -> None:
    parser = argparse.ArgumentParser(
        description='race HTML の取得・存在確認・rawdata 更新をまとめて行うデバッグ兼更新スクリプト'
    )
    parser.add_argument('--from-date', default=DEFAULT_START, help='開始日。例: 2010/01/01')
    parser.add_argument('--to-date', default=DEFAULT_END, help='終了日。例: 2026/04/19')
    parser.add_argument('--waiting-time', type=int, default=10, help='race_id取得時の Selenium wait 秒数')
    parser.add_argument('--calendar-sleep-seconds', type=float, default=1.0, help='calendar アクセス間隔（秒）')
    parser.add_argument('--download-race-html', action='store_true', help='race_id取得後に race HTML も保存する')
    parser.add_argument('--overwrite-html', action='store_true', help='既存HTMLを上書き保存する')
    parser.add_argument('--limit-race-ids', type=int, default=0, help='先頭 N 件の race_id のみに絞る。0 は全件')
    parser.add_argument('--inspect-head-bytes', type=int, default=500, help='先頭ファイルから表示する bytes 数')
    parser.add_argument('--parse-check', action='store_true', help='先頭数件で get_rawdata_* の単体チェックを行う')
    parser.add_argument('--parse-check-limit', type=int, default=3, help='parse-check 対象件数')
    parser.add_argument('--debug-only', action='store_true', help='rawdata 更新は行わず、取得と確認のみ行う')
    args = parser.parse_args()

    tmp_dir = getattr(LocalPaths, 'TMP_DIR', os.path.join(LocalPaths.DATA_DIR, 'tmp'))
    os.makedirs(tmp_dir, exist_ok=True)

    start_label = args.from_date.replace('/', '').replace('-', '')
    end_label = args.to_date.replace('/', '').replace('-', '')
    kaisai_csv_path = os.path.join(tmp_dir, f'kaisai_date_{start_label}_{end_label}.csv')
    race_id_csv_path = os.path.join(tmp_dir, f'race_id_list_{start_label}_{end_label}.csv')
    html_path_csv_path = os.path.join(tmp_dir, f'race_html_files_{start_label}_{end_label}.csv')

    kaisai_date_list = preparing.scrape_kaisai_date(
        from_=args.from_date,
        to_=args.to_date,
        sleep_seconds=args.calendar_sleep_seconds,
    )
    pd.DataFrame({'kaisai_date': kaisai_date_list}).to_csv(kaisai_csv_path, index=False, encoding='utf-8-sig')
    print(f'kaisai dates saved: {kaisai_csv_path} ({len(kaisai_date_list)} rows)')

    race_id_list = preparing.scrape_race_id_list(
        kaisai_date_list,
        waiting_time=args.waiting_time,
        save_csv_path=race_id_csv_path,
        continue_on_error=True,
        dedupe=True,
    )
    print(f'race ids fetched: {len(race_id_list)}')

    if args.limit_race_ids and args.limit_race_ids > 0:
        race_id_list = race_id_list[: args.limit_race_ids]
        print(f'race ids limited: {len(race_id_list)}')

    pd.DataFrame({'race_id': race_id_list}).to_csv(race_id_csv_path, index=False, encoding='utf-8-sig')
    print(f'race ids saved: {race_id_csv_path} ({len(race_id_list)} rows)')

    downloaded_html_paths: List[str] = []
    if args.download_race_html:
        if not callable(getattr(preparing, 'scrape_html_race', None)):
            raise ImportError('scrape_html_race() を読み込めません。requirements.txt の依存関係を確認してください。')
        downloaded_html_paths = preparing.scrape_html_race(
            race_id_list,
            skip=not args.overwrite_html,
        )
        print(f'race html saved/returned: {len(downloaded_html_paths)} files')

    resolved_html_files, missing_race_ids, race_html_dir = _resolve_local_race_html_paths(
        race_id_list,
        downloaded_paths=downloaded_html_paths,
    )
    print(f'race html dir: {race_html_dir}')
    print(f'local race html resolved: {len(resolved_html_files)} / {len(race_id_list)}')
    if missing_race_ids:
        print(f'missing race html ids: {len(missing_race_ids)}')
        print(f'missing sample: {missing_race_ids[:10]}')

    pd.DataFrame({'html_path': resolved_html_files}).to_csv(html_path_csv_path, index=False, encoding='utf-8-sig')
    print(f'race html paths saved: {html_path_csv_path} ({len(resolved_html_files)} rows)')

    _print_html_debug_info(resolved_html_files, head_bytes=args.inspect_head_bytes)

    if args.parse_check:
        _run_single_file_parse_check(resolved_html_files, args.parse_check_limit)

    if args.debug_only:
        print('debug-only mode enabled. rawdata update skipped.')
        return

    if not resolved_html_files:
        raise FileNotFoundError(
            'race HTML を 1 件も解決できませんでした。download-race-html / overwrite-html / 保存先パスを確認してください。'
        )

    print('building raw results table...')
    results_new = preparing.get_rawdata_results(resolved_html_files)
    print(f'results_new shape: {getattr(results_new, "shape", None)}')

    print('building raw race info table...')
    race_info_new = preparing.get_rawdata_info(resolved_html_files)
    print(f'race_info_new shape: {getattr(race_info_new, "shape", None)}')

    print('building raw return table...')
    return_tables_new = preparing.get_rawdata_return(resolved_html_files)
    print(f'return_tables_new shape: {getattr(return_tables_new, "shape", None)}')

    print('updating raw tables...')
    preparing.update_rawdata(filepath=LocalPaths.RAW_RESULTS_PATH, new_df=results_new)
    preparing.update_rawdata(filepath=LocalPaths.RAW_RACE_INFO_PATH, new_df=race_info_new)
    preparing.update_rawdata(filepath=LocalPaths.RAW_RETURN_TABLES_PATH, new_df=return_tables_new)
    print('raw tables updated successfully.')


if __name__ == '__main__':
    main()
