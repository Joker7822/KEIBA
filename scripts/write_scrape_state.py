from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

STATE_DIR = Path('.github/state')
INITIAL_MARKER = STATE_DIR / 'initial_full_run.done'
LAST_SUCCESS_FILE = STATE_DIR / 'last_successful_run_jst.txt'
LAST_RUN_JSON = STATE_DIR / 'last_run.json'


def jst_now() -> dt.datetime:
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo('Asia/Tokyo'))
    except Exception:
        return dt.datetime.now(dt.timezone(dt.timedelta(hours=9)))


def main() -> None:
    parser = argparse.ArgumentParser(description='Persist scrape state files')
    parser.add_argument('--run-mode', required=True)
    parser.add_argument('--from-date', required=True)
    parser.add_argument('--to-date', required=True)
    args = parser.parse_args()

    now = jst_now()
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    LAST_SUCCESS_FILE.write_text(args.to_date, encoding='utf-8')

    if args.run_mode == 'full':
        INITIAL_MARKER.write_text(
            f'completed_at_jst={now.isoformat()}\nfrom_date={args.from_date}\nto_date={args.to_date}\n',
            encoding='utf-8',
        )

    LAST_RUN_JSON.write_text(
        json.dumps(
            {
                'completed_at_jst': now.isoformat(),
                'run_mode': args.run_mode,
                'from_date': args.from_date,
                'to_date': args.to_date,
            },
            ensure_ascii=False,
            indent=2,
        ) + '\n',
        encoding='utf-8',
    )


if __name__ == '__main__':
    main()
