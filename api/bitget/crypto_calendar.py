import requests
import json
from datetime import datetime, timezone
from utils import crypto_event_utils as ceu
import time
import random
import pandas as pd
from datetime import datetime, timezone, timedelta

BITGET_URL = "https://www.bitget.com/v1/cms/crypto/calendar/events/daily"

def date_to_ms_utc(date_str: str) -> int:
    """
    'YYYY-MM-DD' → 해당 날짜 00:00:00 UTC의 epoch milliseconds
    (사이트가 UTC 기준 ms 타임스탬프를 받는 것으로 보일 때 사용)
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def bitget_calendar_to_df(payload: dict) -> pd.DataFrame:
    """
    Bitget 캘린더 API의 JSON 응답에서
    주요 열만 추출해 pandas DataFrame으로 변환.

    남기는 열:
      id, title, categories, coin_name, coin_symbol,
      start_time_kst, link, source
    """
    def ms_to_kst(ms):
        if ms is None:
            return None
        # ms → datetime (UTC → KST)
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        return dt.astimezone(timezone(timedelta(hours=9)))

    items = payload.get("data", {}).get("items", [])
    rows = []
    for ev in items:
        coin = ev.get("coin") or {}
        rows.append({
            "id": ev.get("id"),
            "title": ev.get("title"),
            "categories": ", ".join(ev.get("categories", [])),
            "coin_name": coin.get("name"),
            "coin_symbol": coin.get("symbol"),
            "start_time_kst": ms_to_kst(ev.get("startTime")),
            "link": ev.get("link"),
            "source": ev.get("source"),
        })

    return pd.DataFrame(rows)


def fetch_bitget_calendar_daily(
    date_ms: int,
    page_num: int = 1,
    page_size: int = 10,
    language_type: int = 0,  # 0=en, 사이트 스펙에 맞게
    language_id: int = 0,    # 0=en, 사이트 스펙에 맞게
    category_name: str = "",
    cookies: str | None = None,
    extra_headers: dict | None = None,
    timeout: int = 20,
):
    """
    Bitget 캘린더 일간 데이터 (requests 버전)
    - date_ms: 예) 1757462400000 (ms epoch)
    - page_num/page_size: 페이지네이션
    - language_type/language_id/category_name: 네트워크 탭 payload 그대로
    - cookies: 브라우저에서 복사한 쿠키 문자열(필요 시)
    - extra_headers: deviceid/terminalcode/tm/uhti 등 추가 헤더(필요 시)
    """
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://www.bitget.com",
        "referer": "https://www.bitget.com/calendar",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
    }
    if cookies:
        headers["cookie"] = cookies
    if extra_headers:
        headers.update(extra_headers)

    payload = {
        "languageType": language_type,
        "pageNum": page_num,
        "pageSize": page_size,
        "params": {
            "date": date_ms,           # ← 예: 1757462400000
            "languageId": language_id, # ← 예: 0
            "categoryName": category_name,  # 빈 문자열 가능
        },
    }

    r = requests.post(BITGET_URL, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()  # HTTP 4xx/5xx면 예외

    # JSON 파싱
    try:
        data = r.json()
    except Exception:
        # 디버깅용
        raise RuntimeError(f"JSON decode 실패: {r.text[:500]}")

    return data


def fetch_crypto_calendar_daily(
    date: str, # 'YYYY-MM-DD'
    page_num: int = 1,
    page_size: int = 10,
    language_type: int = 0,  # 0=en, 사이트 스펙에 맞게
    language_id: int = 0,    # 0=en, 사이트 스펙에 맞게
    category_name: str = "",
    cookies: str | None = None,
    extra_headers: dict | None = None,
    timeout: int = 20,
):
    """
    Bitget 캘린더 일간 데이터 (requests 버전)
    - date_ms: 예) 1757462400000 (ms epoch)
    - page_num/page_size: 페이지네이션
    - language_type/language_id/category_name: 네트워크 탭 payload 그대로
    - cookies: 브라우저에서 복사한 쿠키 문자열(필요 시)
    - extra_headers: deviceid/terminalcode/tm/uhti 등 추가 헤더(필요 시)
    """
    date_ms = date_to_ms_utc(date)
    data = fetch_bitget_calendar_daily(
        date_ms=date_ms,
        page_num=page_num,
        page_size=page_size,
        language_type=language_type,
        language_id=language_id,
        category_name=category_name,
        cookies=cookies,
        extra_headers=extra_headers,
        timeout=timeout,
    )
    result = bitget_calendar_to_df(data)
    return result


def fetch_crypto_calendar_range(start_date: str, end_date: str, page_size: int = 100) -> pd.DataFrame:
    """
    Bitget crypto calendar를 날짜 범위로 수집해 하나의 DataFrame으로 반환.
    - start_date, end_date: 'YYYY-MM-DD' (둘 다 포함, inclusive)
    - 일자별 요청 사이에 짧은 지연 추가(봇 필터 완화)
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    if end_dt < start_dt:
        raise ValueError("end_date가 start_date보다 앞일 수 없습니다.")

    frames = []
    cur = start_dt
    while cur <= end_dt:
        day_str = cur.strftime("%Y-%m-%d")
        try:
            day_data = fetch_crypto_calendar_daily(day_str, page_size=page_size)
            if isinstance(day_data, list):
                day_df = pd.DataFrame(day_data)
            else:
                day_df = day_data if isinstance(day_data, pd.DataFrame) else pd.DataFrame()
            if not day_df.empty:
                frames.append(day_df)
        except Exception as e:
            print(f"[crypto][{day_str}] fetch error: {e}")
        finally:
            # 봇 차단 완화용 지연
            time.sleep(0.5 + random.random() * 0.7)
        cur += timedelta(days=1)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    if "id" in out.columns:
        out = out.drop_duplicates(subset=["id"])
    else:
        out = out.drop_duplicates()
    return out