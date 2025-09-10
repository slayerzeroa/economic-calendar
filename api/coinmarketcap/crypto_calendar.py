import requests

def get_cmc_session():
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://coinmarketcap.com",
        "Referer": "https://coinmarketcap.com/events/",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/139.0.0.0 Safari/537.36"),
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Platform": "web",
    })

    # 이벤트 페이지 방문해서 토큰 확보
    r = s.get("https://coinmarketcap.com/events/", timeout=5)

    csrf = s.cookies.get("x-csrf-token")
    if not csrf:
        raise RuntimeError("x-csrf-token을 못 가져왔습니다. 응답 확인 필요")

    # 헤더와 쿠키 둘 다 반영
    s.headers["x-csrf-token"] = csrf
    s.cookies.set("x-csrf-token", csrf, domain=".coinmarketcap.com")

    return s

# 사용 예시
if __name__ == "__main__":
    session = get_cmc_session()
    print("CSRF token:", session.headers["x-csrf-token"])
