from __future__ import annotations

import argparse
import datetime as dt
import os

DEFAULT_START = '2010/01/01'


def jst_now() -> dt.datetime:
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo('Asia/Tokyo'))
    except Exception:
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))


def fmt_date(value: dt.date) -> str:
    return value.strftime('%Y/%m/%d')


def parse_date(value: str) -> dt.date:
    for fmt in ('%Y/%m/%d', '%Y-%m-%d', '%Y%m%d'):
        try:
            return dt.datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f'Unsupported date format: {value}')


def emit(name: str, value: str) -> None:
    print(f'{name}={value}')
    github_output = os.environ.get('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a', encoding='utf-8') as f:
            f.write(f'{name}={value}\n')


def main() -> None:
    parser = argparse.ArgumentParser(description='Resolve scrape date range for GitHub Actions')
    parser.add_argument('--default-start', default=DEFAULT_START)
    parser.add_argument('--today', default='')
    args = parser.parse_args()

    today = parse_date(args.today) if args.today else jst_now().date()
    default_start = parse_date(args.default_start)

    emit('run_mode', 'full')
    emit('from_date', fmt_date(default_start))
    emit('to_date', fmt_date(today))
    emit('from_date_compact', default_start.strftime('%Y%m%d'))
    emit('to_date_compact', today.strftime('%Y%m%d'))
    emit('today_jst', fmt_date(today))
    emit('initial_done', 'true')


if __name__ == '__main__':
    main()
