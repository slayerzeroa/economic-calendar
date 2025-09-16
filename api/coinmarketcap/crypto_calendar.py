import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc

EVENTS_URL = "https://coinmarketcap.com/events/"
POST_URL   = "https://api.coinmarketcap.com/data-api/v3/calendar/query"

def crawl_cmc_events(start_date="2025-09-10", end_date="2025-09-24", page=1, size=20):
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--lang=ko-KR")
    chrome_options.add_argument("--window-size=1200,900")
    # chrome_options.add_argument("--headless=new")  # 필요하면 headless 모드

    # driver = webdriver.Chrome(
    #     service=Service(ChromeDriverManager().install()),
    #     options=chrome_options
    # )
    driver = uc.Chrome(headless=False)


    try:
        # 1) 이벤트 페이지 열기 (쿠키/토큰 세팅)
        driver.get(EVENTS_URL)
        time.sleep(5)  # 페이지 초기 스크립트가 쿠키 심을 시간
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        # 2) 페이지 안에서 fetch 실행 → JSON 문자열 반환
        script = """
        const callback = arguments[arguments.length - 1];
        const url = arguments[0];
        const payload = arguments[1];

        fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
        credentials: 'include'  // 쿠키 포함 (CSRF 토큰 포함)
        })
        .then(res => res.text())
        .then(body => callback(body))
        .catch(err => callback(JSON.stringify({error: String(err)})));
        """
        payload = {
            "startDate": start_date,
            "endDate": end_date,
            "page": page,
            "size": size
        }

        raw = driver.execute_async_script(script, POST_URL, payload)

        # 3) JSON 파싱
        try:
            data = json.loads(raw)
        except Exception:
            print("[DEBUG] raw response snippet:", raw[:500])
            raise

        return data

    finally:
        driver.quit()


if __name__ == "__main__":
    result = crawl_cmc_events()
    print(json.dumps(result, ensure_ascii=False, indent=2))
