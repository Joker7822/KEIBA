import argparse
import os

import pandas as pd

from modules import preparing
from modules.constants import LocalPaths


DEFAULT_START = '2010/01/01'
DEFAULT_END = '2026/04/12'


def main() -> None:
    parser = argparse.ArgumentParser(
        description='指定した期間の開催日と race_id をスクレイピングするスクリプト'
    )
    parser.add_argument('--from-date', default=DEFAULT_START, help='開始日。例: 2010/01/01')
    parser.add_argument('--to-date', default=DEFAULT_END, help='終了日。例: 2026/04/12')
    parser.add_argument('--waiting-time', type=int, default=10, help='race_id取得時の Selenium wait 秒数')
    parser.add_argument('--calendar-sleep-seconds', type=float, default=1.0, help='calendar アクセス間隔（秒）')
    parser.add_argument('--download-race-html', action='store_true', help='race_id取得後に race HTML も保存する')
    parser.add_argument('--overwrite-html', action='store_true', help='既存HTMLを上書き保存する')
    args = parser.parse_args()

    tmp_dir = getattr(LocalPaths, 'TMP_DIR', os.path.join(LocalPaths.DATA_DIR, 'tmp'))
    os.makedirs(tmp_dir, exist_ok=True)

    start_label = args.from_date.replace('/', '').replace('-', '')
    end_label = args.to_date.replace('/', '').replace('-', '')

    kaisai_csv_path = os.path.join(tmp_dir, f'kaisai_date_{start_label}_{end_label}.csv')
    race_id_csv_path = os.path.join(tmp_dir, f'race_id_list_{start_label}_{end_label}.csv')

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
    pd.DataFrame({'race_id': race_id_list}).to_csv(race_id_csv_path, index=False, encoding='utf-8-sig')
    print(f'race ids saved: {race_id_csv_path} ({len(race_id_list)} rows)')

    if args.download_race_html:
        if not callable(getattr(preparing, 'scrape_html_race', None)):
            raise ImportError('scrape_html_race() を読み込めません。requirements.txt の依存関係を確認してください。')
        updated_html_path_list = preparing.scrape_html_race(
            race_id_list,
            skip=not args.overwrite_html,
        )
        print(f'race html saved: {len(updated_html_path_list)} files')


if __name__ == '__main__':
    main()
