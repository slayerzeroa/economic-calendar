# pip install requests beautifulsoup4 python-dateutil pandas

import time
import math
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_dt

AJAX_URL = "https://www.investing.com/economic-calendar/Service/getCalendarFilteredData"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.investing.com",
    "Referer": "https://www.investing.com/economic-calendar/",
}

def _parse_table(html_snippet: str) -> list[dict]:
    """AJAX 응답의 HTML 조각에서 이벤트 행 파싱."""
    soup = BeautifulSoup(html_snippet, "html.parser")
    rows = soup.select("tr.js-event-item")
    out = []
    for r in rows:
        dt_str = r.get("data-event-datetime")  # "YYYY/MM/DD HH:mm:ss"
        # 현지 시간 그대로 표기되므로 필요하면 tz 처리하세요
        dt = parse_dt(dt_str, dayfirst=False)
        impact = len(r.select(".sentiment i.grayFullBullishIcon"))
        cur = r.select_one(".flagCur span")
        cur_title = cur.get("title") if cur else None
        ev_cell = r.select_one("td.event")
        title = ev_cell.select_one("a").get_text(strip=True) if ev_cell else None
        url = ev_cell.select_one("a")["href"] if ev_cell and ev_cell.select_one("a") else None
        actual = (r.select_one("td.bold").get_text(strip=True) if r.select_one("td.bold") else "")
        forecast = (r.select_one("td.fore").get_text(strip=True) if r.select_one("td.fore") else "")
        previous = (r.select_one("td.prev").get_text(strip=True) if r.select_one("td.prev") else "")
        ev_type = None
        if ev_cell:
            if ev_cell.select_one(".smallGrayReport"): ev_type = "report"
            elif ev_cell.select_one(".audioIconNew"):  ev_type = "speech"
            elif ev_cell.select_one(".smallGrayP"):    ev_type = "release"

        out.append({
            "datetime": dt,             # 화면에 보이는 현지시간( 사이트 설정 기준 )
            "currency": cur_title,
            "impact_bulls": impact,     # 황소 아이콘 개수(중요도)
            "title": title,
            "event_url": f"https://www.investing.com{url}" if url and url.startswith("/") else url,
            "actual": actual,
            "forecast": forecast,
            "previous": previous,
            "type": ev_type,
        })
    return out

def fetch_investing_range(start_date: str, end_date: str,
                          tz_offset: int = 0,
                          countries: list[int] | None = None,
                          importances: list[int] | None = None,
                          pause_sec: float = 0.8) -> pd.DataFrame:
    """
    날짜 범위를 움직이며 데이터 수집.
    - start_date, end_date: 'YYYY-MM-DD'
    - tz_offset: 사이트 파라미터 timeZone (예: 한국=+9 → 9)
    - countries: 국가 ID 리스트 (없으면 전체)
    - importances: 중요도(1~3) 리스트 (없으면 전체)
    """
    s = requests.Session()
    s.headers.update(HEADERS)

    # investing은 길게 줘도 한 번에 내려주지만, 서버 안정성을 위해 일자 단위로 쪼개 수집
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()

    all_rows: list[dict] = []
    total_days = (d1 - d0).days + 1
    for i in range(total_days):
        d = d0 + timedelta(days=i)
        payload = {
            "dateFrom": d.strftime("%Y-%m-%d"),
            "dateTo": d.strftime("%Y-%m-%d"),
            "timeZone": tz_offset,
            "limit_from": 0,
        }
        # 배열 파라미터는 키 뒤에 [] 필요
        if countries:
            for idx, c in enumerate(countries):
                payload[f"country[{idx}]"] = c
        if importances:
            for idx, imp in enumerate(importances):
                payload[f"importance[{idx}]"] = imp

        r = s.post(AJAX_URL, data=payload, timeout=20)
        r.raise_for_status()
        j = r.json()
        html = j.get("data", "")
        if html:
            rows = _parse_table(html)
            all_rows.extend(rows)

        time.sleep(pause_sec)  # 부하/차단 방지

    df = pd.DataFrame(all_rows)
    # 보기 좋은 정렬
    if not df.empty:
        df = df.sort_values(["datetime", "impact_bulls"], ascending=[True, False]).reset_index(drop=True)
    return df

# 사용 예시
if __name__ == "__main__":
    # 오늘 하루
    df_today = fetch_investing_range("2025-09-10", "2025-09-11", tz_offset=9)  # KST 기준
    print(df_today)
    