from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path

DEFAULT_START = '2010/01/01'
STATE_DIR = Path('.github/state')
INITIAL_MARKER = STATE_DIR / 'initial_full_run.done'
LAST_SUCCESS_FILE = STATE_DIR / 'last_successful_run_jst.txt'


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


def read_last_success() -> dt.date | None:
    if not LAST_SUCCESS_FILE.exists():
        return None
    raw = LAST_SUCCESS_FILE.read_text(encoding='utf-8').strip()
    if not raw:
        return None
    return parse_date(raw)


def emit(name: str, value: str) -> None:
    print(f'{name}={value}')
    github_output = os.environ.get('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a', encoding='utf-8') as f:
            f.write(f'{name}={value}\n')


def main() -> None:
    parser = argparse.ArgumentParser(description='Resolve scrape date range for GitHub Actions')
    parser.add_argument('--mode', choices=['auto', 'full', 'incremental'], default='auto')
    parser.add_argument('--default-start', default=DEFAULT_START)
    parser.add_argument('--overlap-days', type=int, default=1)
    parser.add_argument('--today', default='')
    args = parser.parse_args()

    today = parse_date(args.today) if args.today else jst_now().date()
    default_start = parse_date(args.default_start)
    last_success = read_last_success()
    initial_done = INITIAL_MARKER.exists()

    if args.mode == 'full':
        run_mode = 'full'
        from_date = default_start
    elif args.mode == 'incremental':
        run_mode = 'incremental'
        if last_success is None:
            from_date = max(default_start, today - dt.timedelta(days=max(1, args.overlap_days)))
        else:
            from_date = max(default_start, last_success - dt.timedelta(days=max(0, args.overlap_days)))
    else:
        if not initial_done:
            run_mode = 'full'
            from_date = default_start
        else:
            run_mode = 'incremental'
            if last_success is None:
                from_date = max(default_start, today - dt.timedelta(days=max(1, args.overlap_days)))
            else:
                from_date = max(default_start, last_success - dt.timedelta(days=max(0, args.overlap_days)))

    to_date = today

    emit('run_mode', run_mode)
    emit('from_date', fmt_date(from_date))
    emit('to_date', fmt_date(to_date))
    emit('from_date_compact', from_date.strftime('%Y%m%d'))
    emit('to_date_compact', to_date.strftime('%Y%m%d'))
    emit('today_jst', fmt_date(today))
    emit('initial_done', 'true' if initial_done else 'false')


if __name__ == '__main__':
    main()
