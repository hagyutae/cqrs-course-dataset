"""
서울 사용자 합성 데이터 생성기
- user_account.json
- user_profile.json

환경변수 (.env 또는 환경):
- DATA_DIR (기본: ./data)
- USER_COUNT (기본: 5000)
"""

import os
import json
import random
import string
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set

from dotenv import load_dotenv

# ---- 설정 -------------------------------------------------------------------

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

try:
    USER_COUNT = int(os.getenv("USER_COUNT", "5000"))
except ValueError:
    USER_COUNT = 5000

RANDOM_SEED = 1337
random.seed(RANDOM_SEED)

# ---- 상수 -------------------------------------------------------------------

EMAIL_DOMAINS = [
    "gmail.com", "naver.com", "daum.net", "kakao.com",
    "outlook.com", "icloud.com", "yahoo.com"
]

KOR_SYLLABLES = list("가나다라마바사아자차카타파하허호희휴효혜예요유윤연영예우은은정준진지지수서선성세소송승시신아연영윤예유은주지하현호희환훈휘효")

KOR_NICKS_PREFIX = [
    "푸른", "행복한", "웃는", "조용한", "작은", "큰", "느린", "빠른", "새벽의",
    "저녁의", "달빛", "햇살", "초록", "파랑", "노을", "봄날", "가을", "겨울",
    "따뜻한", "진지한", "싱그런", "산뜻한", "정다운", "기쁜"
]

KOR_NICKS_NOUN = [
    "고래", "여우", "판다", "수달", "고양이", "강아지", "고슴도치", "토끼",
    "참새", "부엉이", "펭귄", "코알라", "다람쥐", "돌고래", "스라소니", "늑대",
    "호랑이", "사자", "치타", "여치", "달팽이", "노루", "사슴", "두더지"
]

# ---- 유틸 -------------------------------------------------------------------

def now_ts() -> str:
    """현재 시각 문자열."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def rand_date_recent(years: int = 3) -> datetime:
    """최근 N년 사이 임의 날짜."""
    now = datetime.now()
    start = now - timedelta(days=365 * years)
    return start + timedelta(seconds=random.randint(0, int((now - start).total_seconds())))


def salted_sha256(text: str, salt_len: int = 8) -> str:
    """텍스트에 솔트를 더한 SHA-256 해시."""
    salt = ''.join(random.choices(string.ascii_letters + string.digits, k=salt_len))
    h = hashlib.sha256((salt + text).encode("utf-8")).hexdigest()
    return f"sha256${salt}${h}"


def gen_phone() -> str:
    """010-XXXX-XXXX 형식 전화번호 생성."""
    return f"010-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"


def gen_username(existing: Set[str]) -> str:
    """영문 소문자+숫자 6~12자, 중복 방지."""
    while True:
        uname = ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(6, 12)))
        if uname not in existing:
            existing.add(uname)
            return uname


def gen_email(username: str, existing: Set[str]) -> str:
    """username 기반 이메일 생성, 중복 시 숫자 suffix."""
    base = f"{username}@{random.choice(EMAIL_DOMAINS)}"
    if base not in existing:
        existing.add(base)
        return base
    n = 1
    while True:
        cand = f"{username}{n}@{random.choice(EMAIL_DOMAINS)}"
        if cand not in existing:
            existing.add(cand)
            return cand
        n += 1


def gen_nickname(existing: Set[str]) -> str:
    """(접두어+명사) 또는 랜덤 한글 2~4자, 중복 시 숫자 suffix."""
    if random.random() < 0.65:
        nick = f"{random.choice(KOR_NICKS_PREFIX)} {random.choice(KOR_NICKS_NOUN)}"
    else:
        nick = ''.join(random.choices(KOR_SYLLABLES, k=random.randint(2, 4)))
    if nick in existing:
        nick = f"{nick}{random.randint(2, 9999)}"
    existing.add(nick)
    return nick


def password_from_username(username: str) -> str:
    """데모용: username 기반 간이 비밀번호 → 해시."""
    raw = f"{username}!{random.randint(10, 99)}"
    return salted_sha256(raw)


def profile_bio(nickname: str) -> str:
    """간단한 프로필 문구 생성."""
    templates = [
        f"{nickname}입니다. 좋은 맛집 함께 찾아요!",
        f"{nickname}의 소소한 일상 기록.",
        f"{nickname} | 커피와 산책을 좋아해요.",
        f"{nickname} | 새로운 메뉴 탐험 중.",
        f"{nickname} | 오늘도 든든하게!",
        f"{nickname} | 음식 사진 찍는 걸 좋아합니다."
    ]
    return random.choice(templates)

# ---- 메인 생성 로직 ----------------------------------------------------------

def generate_users():
    """user_account.json / user_profile.json 생성."""
    users: List[Dict] = []
    profiles: List[Dict] = []

    username_set: Set[str] = set()
    email_set: Set[str] = set()
    nickname_set: Set[str] = set()

    for uid in range(1, USER_COUNT + 1):
        created_dt = rand_date_recent(3)
        created_ts = created_dt.strftime("%Y-%m-%d %H:%M:%S")

        username = gen_username(username_set)
        email = gen_email(username, email_set)
        phone = gen_phone()
        pw_hash = password_from_username(username)

        users.append({
            "user_id": uid,
            "username": username,
            "password_hash": pw_hash,
            "email": email,
            "phone_number": phone,
            "joined_at": created_ts,
            "is_deleted": False,
            "created_at": created_ts,
            "updated_at": created_ts
        })

        nick = gen_nickname(nickname_set)
        profiles.append({
            "user_id": uid,
            "nickname": nick,
            "image_path": f"/u/{uid}",
            "bio": profile_bio(nick),
            "is_deleted": False,
            "created_at": created_ts,
            "updated_at": created_ts
        })

    write_json(DATA_DIR / "user_account.json", users)
    write_json(DATA_DIR / "user_profile.json", profiles)

    print(f"[OK] Generated users: {len(users)}")
    print(f"     -> {DATA_DIR / 'user_account.json'}")
    print(f"     -> {DATA_DIR / 'user_profile.json'}")

# ---- 출력 헬퍼 ----------------------------------------------------------------

def write_json(path: Path, payload) -> None:
    """JSON 파일 저장."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ---- 엔트리포인트 ------------------------------------------------------------

def main():
    generate_users()


if __name__ == "__main__":
    main()
