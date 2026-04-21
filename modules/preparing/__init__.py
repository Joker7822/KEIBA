from ._scrape_race_id_list import scrape_kaisai_date, scrape_race_id_list

try:
    from ._scrape_html import scrape_html_horse, scrape_html_ped, scrape_html_race, \
        scrape_html_horse_with_master, scrape_horse_id_list_from_search, scrape_html_horse_from_search
except ImportError:
    scrape_html_horse = None
    scrape_html_ped = None
    scrape_html_race = None
    scrape_html_horse_with_master = None
    scrape_horse_id_list_from_search = None
    scrape_html_horse_from_search = None

try:
    from ._get_rawdata import get_rawdata_horse_results, get_rawdata_horse_info, get_rawdata_info, get_rawdata_peds, \
        get_rawdata_results, get_rawdata_return, update_rawdata
except ImportError:
    get_rawdata_horse_results = None
    get_rawdata_horse_info = None
    get_rawdata_info = None
    get_rawdata_peds = None
    get_rawdata_results = None
    get_rawdata_return = None
    update_rawdata = None

try:
    from ._scrape_shutuba_table import scrape_shutuba_table, scrape_horse_id_list
except ImportError:
    scrape_shutuba_table = None
    scrape_horse_id_list = None

try:
    from ._prepare_chrome_driver import prepare_chrome_driver
except ImportError:
    prepare_chrome_driver = None

try:
    from ._create_active_race_id_list import scrape_race_id_race_time_list, create_active_race_id_list
except ImportError:
    # selenium 未導入環境でも、開催日取得やHTML取得系は import できるようにする
    scrape_race_id_race_time_list = None
    create_active_race_id_list = None
