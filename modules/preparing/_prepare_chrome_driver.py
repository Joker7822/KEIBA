import shutil


def prepare_chrome_driver():
    """
    GitHub Actions / ローカルの両方で起動しやすい ChromeDriver 準備。
    1) まず runner にプリインストール済みの Chrome / ChromeDriver を優先利用
    2) 見つからない場合のみ webdriver-manager で取得
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager
    except ImportError as exc:
        raise ImportError(
            'selenium と webdriver-manager が必要です。requirements.txt をインストールしてください。'
        ) from exc

    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')

    chrome_binary = (
        shutil.which('google-chrome')
        or shutil.which('google-chrome-stable')
        or shutil.which('chromium')
        or shutil.which('chromium-browser')
    )
    if chrome_binary:
        options.binary_location = chrome_binary

    chromedriver_binary = shutil.which('chromedriver')
    if chromedriver_binary:
        driver = webdriver.Chrome(service=Service(chromedriver_binary), options=options)
    else:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    driver.set_window_size(1280, 720)
    return driver
