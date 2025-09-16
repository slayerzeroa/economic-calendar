import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# .env 불러오기
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# SQLAlchemy 엔진 생성
DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

def insert_crypto_calendar(df: pd.DataFrame):
    """
    crypto_calendar DataFrame → MySQL 테이블 저장 후 연결 자동 종료
    """
    if df.empty:
        return

    # id 컬럼이 고유키 역할을 하므로 이를 기준으로 중복 제거
    df = df.drop_duplicates(subset=["id"])

    with engine.begin() as conn:
        # DB에 이미 저장된 id 조회
        existing_ids = pd.read_sql(text("SELECT id FROM crypto_calendar"), conn)["id"].tolist()
        # 새로운 id만 필터
        new_df = df[~df["id"].isin(existing_ids)].copy()


    with engine.begin() as conn:   # ← 블록이 끝나면 자동으로 커넥션 반환 및 종료
        new_df.to_sql(
            name="crypto_calendar",
            con=conn,
            if_exists="append",
            index=False,
            chunksize=500
        )


def insert_economic_calendar(df: pd.DataFrame):
    """
    economic_calendar DataFrame → MySQL 테이블 저장 후 연결 자동 종료
    """
    if df.empty:
        return

    df = df.drop_duplicates(subset=["datetime", "title", "event_url", "event_url", "actual", "forecast"])

    with engine.begin() as conn:
        df.to_sql(
            name="economic_calendar",
            con=conn,
            if_exists="append",
            index=False,
            chunksize=500
        )



def insert_crypto_calendar(df: pd.DataFrame):
    """
    crypto_calendar DataFrame → MySQL 테이블에 저장
    DB에 이미 있는 id는 제외하고 나머지만 insert.
    """
    if df.empty:
        return

    with engine.begin() as conn:
        # DB에 이미 저장된 id 조회
        existing_ids = pd.read_sql(text("SELECT id FROM crypto_calendar"), conn)["id"].tolist()
        # 새로운 id만 필터
        new_df = df[~df["id"].isin(existing_ids)].copy()

        if not new_df.empty:
            new_df.to_sql(
                name="crypto_calendar",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=500,
            )
            print(f"[crypto_calendar] 새로 추가된 행: {len(new_df)}")
        else:
            print("[crypto_calendar] 새로 추가할 행 없음.")


def insert_economic_calendar(df: pd.DataFrame):
    """
    economic_calendar DataFrame → MySQL 테이블에 저장
    DB에 이미 있는 (datetime,currency,title) 조합은 제외하고 나머지만 insert.
    """
    if df.empty:
        return

    with engine.begin() as conn:
        existing = pd.read_sql(
            text("SELECT datetime, currency, title FROM economic_calendar"), conn
        )
        # 중복 키 조합 생성
        key_cols = ["datetime", "currency", "title"]
        df["_key"] = df[key_cols].astype(str).agg("|".join, axis=1)
        existing["_key"] = existing[key_cols].astype(str).agg("|".join, axis=1)

        new_df = df[~df["_key"].isin(existing["_key"])].drop(columns=["_key"])

        if not new_df.empty:
            new_df.to_sql(
                name="economic_calendar",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=500,
            )
            print(f"[economic_calendar] 새로 추가된 행: {len(new_df)}")
        else:
            print("[economic_calendar] 새로 추가할 행 없음.")