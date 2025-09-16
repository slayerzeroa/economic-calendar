import api.bitget.crypto_calendar as bec
import utils.crypto_event_utils as ceu
import json
import pandas as pd

import api.investingcom.economic_calendar as ec
import utils.db as db

import time

start = "2025-09-18"


while start < "2025-09-22":
    try:
        crypto_calendar = bec.fetch_crypto_calendar_daily(start, page_size=100)
        economic_calendar = ec.fetch_investing_range(start, start, tz_offset=9)
        db.insert_crypto_calendar(crypto_calendar)
        db.insert_economic_calendar(economic_calendar)
        print(f"Inserted data for {start}: "
              f"{len(crypto_calendar)} crypto events, "
              f"{len(economic_calendar)} economic events.")
    except Exception as e:
        print(f"Error on {start}: {e}")
        continue
    finally:
        time.sleep(10)
        start = ceu.next_date_str(start)