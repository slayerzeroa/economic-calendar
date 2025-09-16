# pip install google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib
# pip install sqlalchemy pymysql pandas python-dotenv pytz

import os, json
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
from datetime import timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pytz
from datetime import datetime, timedelta
from sqlalchemy import text


# -------------------- .env & DB --------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")

DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

# ----------------- Google Calendar -----------------
SCOPES = ["https://www.googleapis.com/auth/calendar"]

# 서비스계정 키 JSON 경로를 .env로 관리 권장
# 예: GOOGLE_SERVICE_ACCOUNT_FILE=/absolute/path/service_account.json
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "api/google/google_calendar_credentials.json")

# 서비스계정은 보통 공유 캘린더에만 쓰기가 됩니다.
# 구글 캘린더 설정 > "캘린더 통합" > 캘린더 ID 복사 (형태: ...@group.calendar.google.com)
CRYPTO_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_CRYPTO_ID", "REPLACE_ME@group.calendar.google.com")
ECONOMIC_CALENDAR_ID = os.getenv("GOOGLE_CALENDAR_ECONOMIC_ID")
# (선택) Workspace 도메인 전체 위임을 사용한다면 임퍼소네이션 대상
# SUBJECT_USER = os.getenv("GOOGLE_IMPERSONATE")  # 예: "user@your-domain.com"

# 파일/형식 검증
p = Path(SERVICE_ACCOUNT_FILE)
if not p.exists():
    raise FileNotFoundError(f"서비스계정 파일이 없습니다: {p.resolve()}")

with p.open("r", encoding="utf-8") as f:
    info = json.load(f)

required_keys = {"type", "client_email", "token_uri", "private_key"}
missing = [k for k in required_keys if k not in info]
if info.get("type") != "service_account" or missing:
    raise ValueError(
        f"올바른 서비스계정 JSON이 아닙니다. 누락: {missing}\n"
        f"Google Cloud Console > IAM & Admin > Service Accounts > Keys에서 새 JSON 키를 받으세요."
    )

credentials = service_account.Credentials.from_service_account_file(
    str(p), scopes=SCOPES
)
# (선택) 도메인 위임 + 임퍼소네이션
# if SUBJECT_USER:
#     credentials = credentials.with_subject(SUBJECT_USER)
# -----------UTILS-----------------
service = build("calendar", "v3", credentials=credentials)

KST = pytz.timezone("Asia/Seoul")

def _to_kst_aware(dt_like) -> str:
    """
    DB 'start_time_kst'가 문자열/naive인 경우 KST로 tz-aware 변환 후 RFC3339 문자열 반환.
    """
    if pd.isna(dt_like):
        return None
    dt = pd.to_datetime(dt_like)
    # 만약 이미 tz-aware면 그대로, 아니면 KST로 로컬라이즈
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        dt = KST.localize(dt)
    return dt.isoformat()


KST = pytz.timezone("Asia/Seoul")

def _to_ts(v):  # str/datetime → pandas.Timestamp (tz-aware KST)
    if v is None or v == "":
        return None
    ts = pd.to_datetime(v)
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        ts = KST.localize(ts)
    return ts

def _build_time_fields(row, start_key, end_key=None, tz="Asia/Seoul", fallback_hours=1):
    """row에서 시작/종료 시각을 읽어 GCal payload(start/end dict)와 iso 시각(중복체크용) 생성."""
    def _parse(v):
        if pd.isna(v) or v is None or v == "":
            return None
        return pd.to_datetime(v)

    start_dt = _parse(row.get(start_key))
    end_dt   = _parse(row.get(end_key)) if end_key else None

    tzinfo = pytz.timezone(tz)
    def _ensure_tz(dt):
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return tzinfo.localize(dt)
        return dt

    if start_dt is None:
        return None, None, None, None

    if end_dt is None:
        end_dt = start_dt + timedelta(hours=fallback_hours)

    # 날짜만 들어온 케이스는 date 이벤트로 처리
    def _is_date_only(s):
        return isinstance(s, str) and len(s) == 10  # 'YYYY-MM-DD'

    if _is_date_only(str(row.get(start_key))) and (end_key and _is_date_only(str(row.get(end_key)))):
        start_date = pd.to_datetime(row.get(start_key)).date()
        end_date   = (pd.to_datetime(row.get(end_key)) + pd.Timedelta(days=1)).date()
        start_iso_dt = tzinfo.localize(pd.Timestamp(start_date).to_pydatetime()).isoformat()
        end_iso_dt   = tzinfo.localize(pd.Timestamp(end_date).to_pydatetime()).isoformat()
        return {"date": start_date.isoformat()}, {"date": end_date.isoformat()}, start_iso_dt, end_iso_dt

    start_dt = _ensure_tz(start_dt)
    end_dt   = _ensure_tz(end_dt)
    return (
        {"dateTime": start_dt.isoformat(), "timeZone": tz},
        {"dateTime": end_dt.isoformat(),   "timeZone": tz},
        start_dt.isoformat(),
        end_dt.isoformat()
    )

# ------------------ Push to GCal -------------------

def push_crypto_events_to_gcal():
    """
    crypto_calendar 테이블의 이벤트를 Google Calendar로 생성.
    - 중복 방지: 같은 제목 & 같은 시작 시각을 1분 창으로 조회해 있으면 skip
    """
    # 캘린더 ID 미설정 방지
    if CRYPTO_CALENDAR_ID.startswith("REPLACE_ME"):
        raise RuntimeError("환경변수 GOOGLE_CALENDAR_ID를 공유 캘린더 ID로 설정하세요.")

    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM crypto_calendar", conn)

    if df.empty:
        print("[crypto] DB에 데이터가 없습니다.")
        return

    for _, row in df.iterrows():
        start_iso = _to_kst_aware(row.get("start_time_kst"))
        if not start_iso:
            continue

        # 기본 1시간 이벤트
        # iso → pandas → timedelta → 다시 iso (tz 유지)
        start_dt = pd.to_datetime(start_iso)
        end_dt = start_dt + timedelta(hours=1)
        end_iso = end_dt.isoformat()

        summary = f"[Crypto] {row.get('title') or ''}".strip()
        description = f"{row.get('link') or ''}\n\n" \
                      f"Source: {row.get('source') or ''}\n" \
                      f"Coin: {row.get('coin_name') or ''} ({row.get('coin_symbol') or ''})"

        event_body = {
            "summary": summary[:300],  # 너무 길면 잘라주기
            "description": description[:8000],
            "start": {"dateTime": start_iso, "timeZone": "Asia/Seoul"},
            "end":   {"dateTime": end_iso,   "timeZone": "Asia/Seoul"},
        }

        # 중복 체크(1분 창)
        try:
            dup = service.events().list(
                calendarId=CRYPTO_CALENDAR_ID,
                timeMin=start_iso,
                timeMax=(start_dt + timedelta(minutes=1)).isoformat(),
                q=row.get("title") or ""
            ).execute()
            if dup.get("items"):
                print(f"[crypto] 이미 존재: {summary} @ {start_iso}")
                continue
        except Exception as e:
            print(f"[crypto] 중복 조회 실패: {e}. 계속 진행합니다.")

        # 생성
        try:
            created = service.events().insert(calendarId=CRYPTO_CALENDAR_ID, body=event_body).execute()
            print(f"[crypto] 등록됨: {created.get('htmlLink')}")
        except Exception as e:
            print(f"[crypto] 생성 실패: {summary} @ {start_iso}\n  -> {e}")


def push_economic_events_to_gcal():
    """
    economic_calendar 테이블의 이벤트를 Google Calendar로 생성.
    - 중복 방지: 같은 제목 & 같은 시작 시각을 1분 창으로 조회해 있으면 skip
    """
    if ECONOMIC_CALENDAR_ID.startswith("REPLACE_ME") or not ECONOMIC_CALENDAR_ID:
        raise RuntimeError("환경변수 GOOGLE_CALENDAR_ID를 공유 캘린더 ID로 설정하세요.")

    with engine.begin() as conn:
        df = pd.read_sql("SELECT * FROM economic_calendar", conn)

    if df.empty:
        print("[economic] DB에 데이터가 없습니다.")
        return

    for _, row in df.iterrows():
        # economic_calendar에는 'datetime' 컬럼을 사용
        # _to_kst_aware가 프로젝트에 이미 있다면 그대로 사용합니다.
        # 없으면 pd.to_datetime(row['datetime'])로 대체하세요.
        start_iso = _to_kst_aware(row.get("datetime"))
        if not start_iso:
            continue

        start_dt = pd.to_datetime(start_iso)
        end_dt = start_dt + timedelta(hours=1)  # 기본 1시간
        end_iso = end_dt.isoformat()

        currency = (row.get("currency") or "").strip()
        title    = (row.get("title") or "").strip()

        summary = f"[Economic] {currency} - {title}".strip()
        description = (
            f"{row.get('event_url') or ''}\n\n"
            f"Impact (bulls): {row.get('impact_bulls') if pd.notna(row.get('impact_bulls')) else ''}\n"
            f"Forecast: {row.get('forecast') or ''}\n"
            f"Actual: {row.get('actual') or ''}\n"
            f"Previous: {row.get('previous') or ''}"
        )

        event_body = {
            "summary": summary[:300],
            "description": description[:8000],
            "start": {"dateTime": start_iso, "timeZone": "Asia/Seoul"},
            "end":   {"dateTime": end_iso,   "timeZone": "Asia/Seoul"},
        }

        # 중복 체크(1분 창)
        try:
            dup = service.events().list(
                calendarId=ECONOMIC_CALENDAR_ID,
                timeMin=start_iso,
                timeMax=(start_dt + timedelta(minutes=1)).isoformat(),
                q=title or ""
            ).execute()
            if dup.get("items"):
                print(f"[economic] 이미 존재: {summary} @ {start_iso}")
                continue
        except Exception as e:
            print(f"[economic] 중복 조회 실패: {e}. 계속 진행합니다.")

        # 생성
        try:
            created = service.events().insert(calendarId=ECONOMIC_CALENDAR_ID, body=event_body).execute()
            print(f"[economic] 등록됨: {created.get('htmlLink')}")
        except Exception as e:
            print(f"[economic] 생성 실패: {summary} @ {start_iso}\n  -> {e}")


def push_crypto_events_to_gcal_range(start, end):
    """
    [start, end) 구간의 crypto_calendar 만 Google Calendar로 생성
    - start/end: 'YYYY-MM-DD' 또는 datetime 가능 (KST 기준)
    """
    if CRYPTO_CALENDAR_ID.startswith("REPLACE_ME"):
        raise RuntimeError("GOOGLE_CALENDAR_CRYPTO_ID를 제대로 설정하세요.")

    start_ts = _to_ts(start)
    end_ts   = _to_ts(end)
    if not start_ts or not end_ts:
        raise ValueError("start/end를 확인하세요.")

    # DB에서 범위로 바로 필터 (성능 ↑)
    sql = text("""
        SELECT * FROM crypto_calendar
        WHERE start_time_kst >= :s AND start_time_kst < :e
        ORDER BY start_time_kst
    """)
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn, params={"s": start_ts.tz_convert(None), "e": end_ts.tz_convert(None)})

    if df.empty:
        print(f"[crypto] 기간 내 데이터 없음: {start_ts} ~ {end_ts}")
        return

    for _, row in df.iterrows():
        start_dict, end_dict, start_iso, _ = _build_time_fields(
            row, start_key="start_time_kst", end_key="end_time_kst", tz="Asia/Seoul", fallback_hours=1
        )
        if not start_iso:
            continue

        summary = f"{row.get('title') or ''}".strip()
        description = f"{row.get('link') or ''}\n\n" \
                      f"Source: {row.get('source') or ''}\n" \
                      f"Coin: {row.get('coin_name') or ''} ({row.get('coin_symbol') or ''})"

        body = {"summary": summary[:300], "description": description[:8000], "start": start_dict, "end": end_dict}

        # 중복 체크(1분 창)
        try:
            dup = service.events().list(
                calendarId=CRYPTO_CALENDAR_ID,
                timeMin=start_iso,
                timeMax=(pd.to_datetime(start_iso) + timedelta(minutes=1)).isoformat(),
                q=row.get("title") or ""
            ).execute()
            if dup.get("items"):
                print(f"[crypto] 이미 존재: {summary} @ {start_iso}")
                continue
        except Exception as e:
            print(f"[crypto] 중복 조회 실패(무시): {e}")

        try:
            created = service.events().insert(calendarId=CRYPTO_CALENDAR_ID, body=body).execute()
            print(f"[crypto] 등록됨: {created.get('htmlLink')}")
        except Exception as e:
            print(f"[crypto] 생성 실패: {summary} @ {start_iso}\n  -> {e}")

def push_economic_events_to_gcal_range(start, end):
    """
    [start, end) 구간의 economic_calendar 만 Google Calendar로 생성
    - start/end: 'YYYY-MM-DD' 또는 datetime 가능 (KST 기준)
    """
    if not ECONOMIC_CALENDAR_ID or ECONOMIC_CALENDAR_ID.startswith("REPLACE_ME"):
        raise RuntimeError("GOOGLE_CALENDAR_ECONOMIC_ID를 제대로 설정하세요.")

    start_ts = _to_ts(start)
    end_ts   = _to_ts(end)
    if not start_ts or not end_ts:
        raise ValueError("start/end를 확인하세요.")

    sql = text("""
        SELECT * FROM economic_calendar
        WHERE `datetime` >= :s AND `datetime` < :e
        ORDER BY `datetime`
    """)
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn, params={"s": start_ts.tz_convert(None), "e": end_ts.tz_convert(None)})

    if df.empty:
        print(f"[economic] 기간 내 데이터 없음: {start_ts} ~ {end_ts}")
        return

    for _, row in df.iterrows():
        start_dict, end_dict, start_iso, _ = _build_time_fields(
            row, start_key="datetime", end_key="end_datetime", tz="Asia/Seoul", fallback_hours=1
        )
        if not start_iso:
            continue

        currency = (row.get("currency") or "").strip()
        title    = (row.get("title") or "").strip()
        summary = f"{currency} - {title}".strip()
        description = (
            f"{row.get('event_url') or ''}\n\n"
            f"Impact (bulls): {row.get('impact_bulls') if pd.notna(row.get('impact_bulls')) else ''}\n"
            f"Forecast: {row.get('forecast') or ''}\n"
            f"Actual: {row.get('actual') or ''}\n"
            f"Previous: {row.get('previous') or ''}"
        )

        body = {"summary": summary[:300], "description": description[:8000], "start": start_dict, "end": end_dict}

        try:
            dup = service.events().list(
                calendarId=ECONOMIC_CALENDAR_ID,
                timeMin=start_iso,
                timeMax=(pd.to_datetime(start_iso) + timedelta(minutes=1)).isoformat(),
                q=title or ""
            ).execute()
            if dup.get("items"):
                print(f"[economic] 이미 존재: {summary} @ {start_iso}")
                continue
        except Exception as e:
            print(f"[economic] 중복 조회 실패(무시): {e}")

        try:
            created = service.events().insert(calendarId=ECONOMIC_CALENDAR_ID, body=body).execute()
            print(f"[economic] 등록됨: {created.get('htmlLink')}")
        except Exception as e:
            print(f"[economic] 생성 실패: {summary} @ {start_iso}\n  -> {e}")


if __name__ == "__main__":
    push_crypto_events_to_gcal_range('2025-09-18', '2025-09-22')
    push_economic_events_to_gcal_range('2025-09-18', '2025-09-22')