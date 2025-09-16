from datetime import datetime, timedelta

def next_date_str(date_str: str, fmt: str = "%Y-%m-%d") -> str:
    """
    입력: 'YYYY-MM-DD' 형태의 문자열
    반환: 다음 날의 'YYYY-MM-DD' 문자열
    """
    d = datetime.strptime(date_str, fmt).date()
    next_d = d + timedelta(days=1)
    return next_d.strftime(fmt)
