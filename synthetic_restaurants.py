"""
서울 지역 식당 데이터 합성기 (JSON 테이블별 출력)

기능 요약
- 식당 이름/설명/카테고리: OpenAI gpt-5-nano로 일괄 생성 (없으면 로컬 폴백)
- 위치/주소/영업시간/전화번호: 로컬 랜덤 생성
- 서울 25개 자치구에 균등 분포, 구 중심 좌표 기준 지터 적용
- 출력: data/restaurant.json, restaurant_location.json, restaurant_image.json, restaurant_category.json
- 환경변수:
  - OPENAI_API_KEY: OpenAI 사용 시 필요(없으면 로컬 폴백)
  - DATABASE_URL: 카테고리를 DB에서 읽을 때 사용(없으면 기본 목록)
"""

from __future__ import annotations

import os
import json
import random
import asyncio
import contextlib
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Tuple

from dotenv import load_dotenv

# ---- 외부 의존 (옵셔널) -----------------------------------------------------

try:
    import psycopg2  # DB 사용 시: pip install psycopg2-binary
except Exception:
    psycopg2 = None  # DB 미사용/미설치 시 폴백

# OpenAI SDK 사용 여부 체크
OPENAI_AVAILABLE = False
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False


# ---- 설정 -------------------------------------------------------------------

load_dotenv()

DATABASE_URL: str = (os.getenv("DATABASE_URL") or "").strip()
OPENAI_API_KEY: str = (os.getenv("OPENAI_API_KEY") or "").strip()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

try:
    NUM_RESTAURANTS = int(os.getenv("NUM_RESTAURANTS", "2000"))
except ValueError:
    NUM_RESTAURANTS = 2000

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

OPENAI_BATCH_SIZE = 50       # OpenAI 1회 요청당 아이템 수
OPENAI_CONCURRENCY = 10      # 동시 요청 수(배치 그룹 크기)


# ---- 상수: 서울 구 좌표/도로/영업시간/카테고리 -------------------------------

SEOUL_DISTRICTS: Dict[str, Tuple[float, float]] = {
    "종로구": (37.5730, 126.9794),  "중구": (37.5636, 126.9976),  "용산구": (37.5326, 126.9905),
    "성동구": (37.5633, 127.0364),  "광진구": (37.5380, 127.0820), "동대문구": (37.5744, 127.0396),
    "중랑구": (37.6060, 127.0927),  "성북구": (37.5894, 127.0167), "강북구": (37.6396, 127.0257),
    "도봉구": (37.6688, 127.0471),  "노원구": (37.6543, 127.0568), "은평구": (37.6176, 126.9227),
    "서대문구": (37.5791, 126.9368), "마포구": (37.5663, 126.9018), "양천구": (37.5169, 126.8664),
    "강서구": (37.5509, 126.8495),  "구로구": (37.4954, 126.8876), "금천구": (37.4569, 126.8956),
    "영등포구": (37.5268, 126.8960), "동작구": (37.5124, 126.9393), "관악구": (37.4784, 126.9516),
    "서초구": (37.4836, 127.0327),  "강남구": (37.5172, 127.0473), "송파구": (37.5146, 127.1059),
    "강동구": (37.5302, 127.1238),
}

DEFAULT_CATEGORIES: List[str] = [
    "한식", "중식", "일식", "양식", "아시아음식",
    "카페/디저트", "패스트푸드", "치킨", "피자", "주점/술집", "기타",
]

DISTRICT_STREETS: Dict[str, List[str]] = {
    "종로구": ["종로", "율곡로", "사직로", "자하문로", "새문안로", "삼청로", "율곡로12길"],
    "중구": ["세종대로", "을지로", "퇴계로", "장충단로", "소공로", "청계천로", "동호로"],
    "용산구": ["한강대로", "이태원로", "후암로", "녹사평대로", "두텁바위로", "한남대로"],
    "성동구": ["왕십리로", "성수일로", "뚝섬로", "무학로", "고산자로", "금호로"],
    "광진구": ["광나루로", "능동로", "자양로", "아차산로", "화양로", "천호대로"],
    "동대문구": ["왕산로", "천호대로", "답십리로", "장한로", "회기로", "이문로"],
    "중랑구": ["면목로", "사가정로", "봉화산로", "망우로", "중랑천로", "상봉로"],
    "성북구": ["성북로", "정릉로", "월계로", "동소문로", "보문로", "장위로"],
    "강북구": ["도봉로", "삼양로", "한천로", "덕릉로", "인수봉로", "수유로"],
    "도봉구": ["도봉로", "마들로", "방학로", "노해로", "시루봉로", "노원로"],
    "노원구": ["노원로", "상계로", "한글비석로", "동일로", "중앙로", "마들로"],
    "은평구": ["연서로", "통일로", "불광로", "역말로", "진흥로", "수색로"],
    "서대문구": ["연세로", "신촌로", "가좌로", "충정로", "모래내로", "독립문로"],
    "마포구": ["마포대로", "월드컵로", "성산로", "독막로", "공덕로", "서강로"],
    "양천구": ["목동로", "오목로", "신월로", "안양천로", "양천로", "중앙로"],
    "강서구": ["화곡로", "공항대로", "방화대로", "곰달래로", "양천로", "가로공원로"],
    "구로구": ["구로중앙로", "경인로", "디지털로", "오리로", "고척로", "구로동로"],
    "금천구": ["시흥대로", "금하로", "독산로", "가산디지털1로", "두산로", "금하로15길"],
    "영등포구": ["국회대로", "여의대로", "영등포로", "도림로", "당산로", "선유로"],
    "동작구": ["노량진로", "흑석로", "장승배기로", "상도로", "동작대로", "알마타길"],
    "관악구": ["관악로", "신림로", "보라매로", "낙성대로", "남부순환로", "난곡로"],
    "서초구": ["서초대로", "반포대로", "사평대로", "양재대로", "남부순환로", "잠원로"],
    "강남구": ["강남대로", "테헤란로", "선릉로", "도산대로", "봉은사로", "언주로", "영동대로"],
    "송파구": ["올림픽로", "송파대로", "위례성대로", "가락로", "석촌호수로", "문정로"],
    "강동구": ["천호대로", "성내로", "양재대로", "올림픽로", "상일로", "둔촌로"],
}

OPENING_HOURS: List[str] = [
    "10:00 ~ 22:00",
    "11:00 ~ 21:30",
    "09:30 ~ 20:00",
    "11:30 ~ 22:00",
    "10:00 ~ 20:00",
    "12:00 ~ 23:00",
    "월-금 11:00-21:00; 토-일 12:00-22:00",
    "매일 10:30-21:30(브레이크 15:00-17:00)",
]


# ---- 유틸 --------------------------------------------------------------------

def now_ts() -> str:
    """현재 시각 문자열(YYYY-MM-DD HH:MM:SS)."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def jitter_coord(lat: float, lon: float, meters: int = 500) -> Tuple[float, float]:
    """구 중심 좌표에 소규모 지터를 더해 위치를 퍼뜨림."""
    lat_j = (meters / 111_000.0) * (random.random() * 2 - 1)
    lon_j = (meters / 88_800.0) * (random.random() * 2 - 1)
    return lat + lat_j, lon + lon_j


def random_phone_seoul() -> str:
    """서울 전화번호 형식 생성."""
    return f"02-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"


def random_opening_hours() -> str:
    """영업시간 랜덤 선택."""
    return random.choice(OPENING_HOURS)


def random_address_line(district: str) -> str:
    """구별 도로명 후보 중 하나로 주소 구성."""
    streets = DISTRICT_STREETS.get(district) or sum(DISTRICT_STREETS.values(), [])
    street = random.choice(streets)
    main_no = random.randint(1, 200)
    sub = f"-{random.randint(1, 50)}" if random.random() < 0.5 else ""
    return f"서울특별시 {district} {street} {main_no}{sub}"


def chunked(seq: List, size: int):
    """리스트를 고정 크기 배치로 분할."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ---- 카테고리 로더 -----------------------------------------------------------

def load_categories() -> List[Dict]:
    """DB에서 카테고리 조회(선택), 없으면 기본값 사용."""
    if not DATABASE_URL or psycopg2 is None:
        return [{"category_id": i + 1, "name": name} for i, name in enumerate(DEFAULT_CATEGORIES)]

    with contextlib.closing(psycopg2.connect(DATABASE_URL)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT category_id, name FROM category "
                "WHERE is_deleted = FALSE ORDER BY category_id;"
            )
            rows = cur.fetchall()
            if not rows:
                return [{"category_id": i + 1, "name": name} for i, name in enumerate(DEFAULT_CATEGORIES)]
            return [{"category_id": r[0], "name": r[1]} for r in rows]


def build_category_maps(cat_rows: List[Dict]):
    """카테고리 이름↔ID 매핑 구성."""
    name_to_id = {c["name"]: c["category_id"] for c in cat_rows}
    id_to_name = {c["category_id"]: c["name"] for c in cat_rows}
    allowed_names = list(name_to_id.keys())
    return name_to_id, id_to_name, allowed_names


# ---- OpenAI 메타데이터 생성 ---------------------------------------------------

class MetaGenerator:
    """식당 이름/설명/카테고리를 일괄 생성. (OpenAI 사용, 실패 시 로컬 폴백)"""

    def __init__(self, api_key: Optional[str], allowed_categories: List[str]):
        self.allowed = allowed_categories
        self.use_openai = OPENAI_AVAILABLE and bool(api_key)
        self.client = OpenAI(api_key=api_key) if self.use_openai else None

    async def generate_batch(
        self,
        count: int,
        categories_hint: List[str],
        districts_hint: List[str],
    ) -> List[Dict]:
        """
        count 만큼의 메타데이터 생성.
        반환 형식: {"name": str, "description": str, "categories": [str,...], "opening_hours": str}
        """
        if not self.use_openai:
            # 폴백: 간단한 이름/설명 생성
            items = [self._fallback_item(random.choice(categories_hint), random.choice(districts_hint)) for _ in range(count)]
            for it in items:
                it["opening_hours"] = random_opening_hours()
            return items

        allow_str = ", ".join(self.allowed)
        sys_msg = (
            "당신은 한국어 식당 메타데이터를 간결히 생성합니다.\n"
            "- JSON 줄 단위로만 반환: {name, description, categories}\n"
            "- categories는 아래 목록에서만 1~3개 선택:\n"
            f"[{allow_str}]\n"
            "- name: 창의적인 한국어 매장 이름(2~12자, 지역 이름 포함 X), description: 100자 이내 1~2 문장(매장 이름 포함 X).\n"
            "- 설명/장식 없이 JSON만 출력."
        )
        user_msg = (
            f"{count}개의 한국 식당 데이터를 만들어주세요.\n"
            f"카테고리 힌트: {', '.join(categories_hint)}\n"
            f"자치구 힌트: {', '.join(districts_hint)}\n"
            "출력은 {name, description, categories} 의 JSON 라인만."
        )

        try:
            # OpenAI 동기 API를 스레드로 감싸 비동기 호환
            resp = await asyncio.to_thread(
                self.client.responses.create,
                model="gpt-5-nano",
                input=[
                    {"role": "system", "content": sys_msg},
                    {"role": "user", "content": user_msg},
                ],
            )
            raw_text = resp.output_text or ""
        except Exception:
            # 완전 폴백
            items = [self._fallback_item(random.choice(categories_hint), random.choice(districts_hint)) for _ in range(count)]
            for it in items:
                it["opening_hours"] = random_opening_hours()
            return items

        items: List[Dict] = []
        for line in raw_text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    continue
                nm, ds, cats = obj.get("name"), obj.get("description"), obj.get("categories")
                if not (nm and ds and isinstance(cats, list) and 1 <= len(cats) <= 3):
                    continue

                # 허용된 카테고리만 유지(없으면 임의 1개)
                filtered = [c for c in cats if c in self.allowed] or [random.choice(self.allowed)]
                items.append({"name": nm, "description": ds, "categories": filtered})
            except Exception:
                # 파싱 실패 라인은 무시
                pass

        # 모델이 충분히 못 준 경우 폴백으로 채움
        while len(items) < count:
            items.append(self._fallback_item(random.choice(categories_hint), random.choice(districts_hint)))

        # 영업시간은 로컬에서 채움
        for it in items:
            it["opening_hours"] = random_opening_hours()

        return items[:count]

    def _fallback_item(self, category: str, district: str) -> Dict:
        """OpenAI 실패 시 사용할 간단한 이름/설명/카테고리 생성."""
        prefix = random.choice(["맛집", "정담", "온기", "담소", "한숲", "소담", "호미", "반가", "다온", "초록", "도란", "한상", "행복", "모락", "미소", "풍미"])
        suffix = random.choice(["식당", "주택", "다방", "분식", "한상", "키친", "포차", "공방", "주점", "테이블", "당"])
        name = f"{prefix}{suffix}"
        desc = f"{district} {category} 전문점"
        cats = [category] if category in self.allowed else [random.choice(self.allowed)]
        return {"name": name, "description": desc, "categories": cats}


# ---- 배치 생성 로직 -----------------------------------------------------------

async def generate_all() -> None:
    """전체 식당/위치/이미지/카테고리 매핑 데이터를 비동기로 생성하고 파일로 저장."""
    categories = load_categories()
    name_to_id, _, allowed_names = build_category_maps(categories)

    # 25개 구로 균등 분포
    districts = list(SEOUL_DISTRICTS.keys())
    assigned_districts = (districts * ((NUM_RESTAURANTS // len(districts)) + 1))[:NUM_RESTAURANTS]
    random.shuffle(assigned_districts)

    generator = MetaGenerator(OPENAI_API_KEY, allowed_categories=allowed_names)

    restaurants: List[Dict] = []
    locations: List[Dict] = []
    images: List[Dict] = []
    rest_cats: List[Dict] = []

    next_restaurant_id = 1

    # OpenAI 요청 배치 구성
    batches = list(chunked(assigned_districts, OPENAI_BATCH_SIZE))
    total_batches = len(batches)

    batch_meta: List[Tuple[int, List[str], List[str]]] = []
    for idx, batch_districts in enumerate(batches, start=1):
        cats_hint = random.sample(allowed_names, k=min(len(allowed_names), 6))
        batch_meta.append((idx, batch_districts, cats_hint))

    # 동시 그룹 단위로 실행
    for start in range(0, total_batches, OPENAI_CONCURRENCY):
        group = batch_meta[start:start + OPENAI_CONCURRENCY]

        # OpenAI 병렬 호출
        results = await asyncio.gather(*[
            generator.generate_batch(len(bd), cats_hint, bd)
            for (_, bd, cats_hint) in group
        ])

        # 결과 소비하며 레코드 빌드
        for (batch_idx, batch_districts, _), meta_batch in zip(group, results):
            for i in range(len(batch_districts)):
                rid = next_restaurant_id
                district = batch_districts[i]
                center = SEOUL_DISTRICTS[district]
                lat, lon = jitter_coord(center[0], center[1], meters=random.randint(150, 650))

                meta = meta_batch[i]
                created_at = now_ts()

                # 식당 기본 정보
                restaurants.append({
                    "restaurant_id": rid,
                    "name": meta["name"],
                    "description": meta["description"],
                    "phone_number": random_phone_seoul(),
                    "opening_hours": meta["opening_hours"],
                    "is_deleted": False,
                    "created_at": created_at,
                    "updated_at": created_at,
                })

                # 위치(1:1)
                locations.append({
                    "restaurant_id": rid,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "address_line": random_address_line(district),
                    "region_si_do": "서울특별시",
                    "region_si_gun_gu": district,
                    "created_at": created_at,
                    "updated_at": created_at,
                })

                # 카테고리(1~3개)
                suggested = meta.get("categories") or []
                picked_ids: List[int] = []
                for cname in suggested:
                    cid = name_to_id.get(cname)
                    if cid and cid not in picked_ids:
                        picked_ids.append(cid)
                if not picked_ids:
                    # 모델이 못 준 경우 로컬 랜덤
                    for cid in random.sample(list(name_to_id.values()), k=random.choice([1, 2, 2, 3])):
                        if cid not in picked_ids:
                            picked_ids.append(cid)

                for cid in picked_ids:
                    rest_cats.append({
                        "rc_id": len(rest_cats) + 1,
                        "restaurant_id": rid,
                        "category_id": cid,
                        "created_at": created_at,
                    })

                # 이미지(1~5장)
                num_imgs = random.choice([1, 2, 3, 3, 3, 4, 5])
                for idx in range(1, num_imgs + 1):
                    images.append({
                        "image_id": len(images) + 1,
                        "restaurant_id": rid,
                        "image_path": f"/{rid}/{idx}",
                        "is_deleted": False,
                        "index": idx - 1,
                        "created_at": created_at,
                        "updated_at": created_at,
                    })

                next_restaurant_id += 1

            print(f"[진행] Batch {batch_idx}/{total_batches} 완료")

        # OpenAI 사용 시 그룹 간 짧은 휴식
        if generator.use_openai:
            await asyncio.sleep(0.2)

    # 파일 저장
    write_json(DATA_DIR / "restaurant.json", restaurants)
    write_json(DATA_DIR / "restaurant_location.json", locations)
    write_json(DATA_DIR / "restaurant_image.json", images)
    write_json(DATA_DIR / "restaurant_category.json", rest_cats)

    print(f"[OK] Generated {len(restaurants)} restaurants")
    print(f"     -> {DATA_DIR / 'restaurant.json'}")
    print(f"     -> {DATA_DIR / 'restaurant_location.json'}")
    print(f"     -> {DATA_DIR / 'restaurant_image.json'}")
    print(f"     -> {DATA_DIR / 'restaurant_category.json'}")
    print(f"OpenAI used: {generator.use_openai}")


# ---- 출력 헬퍼 ----------------------------------------------------------------

def write_json(path: Path, payload) -> None:
    """JSON 파일로 저장(UTF-8, pretty)."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ---- 엔트리포인트 ------------------------------------------------------------

def main() -> None:
    asyncio.run(generate_all())


if __name__ == "__main__":
    main()
