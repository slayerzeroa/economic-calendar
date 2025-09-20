import time
from datetime import datetime, timedelta

import pandas as pd

import api.bitget.crypto_calendar as bec
import api.investingcom.economic_calendar as ec
import utils.db as db

from api.google import google_calendar
from api.google.google_calendar import (
    push_crypto_events_to_gcal_range,
    push_economic_events_to_gcal_range,
)


def main():
    """
    오늘 기준으로 정확히 7일 후의 날짜(하루치)에 대한
    crypto / economic 이벤트를 DB에 저장하고
    Google Calendar에 동기화한다.
    """
    try:
        # 오늘+7일 날짜를 YYYY-MM-DD 문자열로 생성
        target_date = (datetime.now() + timedelta(days=6)).strftime("%Y-%m-%d")
        # 1️. Bitget Crypto Calendar 하루치 수집
        # crypto_df = bec.fetch_crypto_calendar_daily(target_date, page_size=100)
        crypto_df = bec.fetch_crypto_calendar_range(datetime.now().strftime("%Y-%m-%d"), target_date, page_size=100)
        if isinstance(crypto_df, list):
            crypto_df = pd.DataFrame(crypto_df)
        if not crypto_df.empty and "id" in crypto_df.columns:
            crypto_df = crypto_df.drop_duplicates(subset=["id"])

        # 오늘+7일 날짜를 YYYY-MM-DD 문자열로 생성
        target_date = (datetime.now() + timedelta(days=6)).strftime("%Y-%m-%d")
        # 2️. Investing Economic Calendar 하루치 수집
        econ_df = ec.fetch_investing_range(target_date, target_date, tz_offset=9)
        if isinstance(econ_df, list):
            econ_df = pd.DataFrame(econ_df)
        if not econ_df.empty:
            econ_df = econ_df.drop_duplicates(subset=["datetime", "currency", "title"])

        # 3️. DB 저장
        if not crypto_df.empty:
            db.insert_crypto_calendar(crypto_df)
        if not econ_df.empty:
            db.insert_economic_calendar(econ_df)

        print(
            f"[DB] {target_date} 저장 완료: "
            f"{len(crypto_df)} crypto 이벤트, {len(econ_df)} economic 이벤트."
        )

        # 4. Google Calendar 동기화
        # end는 하루 뒤 날짜 문자열
        end_date = (datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

        if not crypto_df.empty:
            push_crypto_events_to_gcal_range(target_date, end_date)
        if not econ_df.empty:
            push_economic_events_to_gcal_range(target_date, end_date)

        print(f"[Google Calendar] {target_date} 등록 완료.")

    except Exception as e:
        print(f"[ERROR] {target_date} 처리 중 에러: {e}")


if __name__ == "__main__":
    main()
