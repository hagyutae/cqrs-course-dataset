"""
리뷰 합성 데이터 생성 파이프라인
- 입력: restaurant.json, user_account.json
- 출력: review_*.json, review_photo_*.json (1000개 단위 청크 파일)
- 규칙: 사용자별 방문일 중복/인접일(±1) 금지
- OpenAI 미사용 시 로컬 문장 생성으로 폴백
"""

from __future__ import annotations

import os
import json
import math
import random
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv


# ---- OpenAI 클라이언트 -------------------------------------------------------

OPENAI_AVAILABLE = False
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False


# ---- 설정 --------------------------------------------------------------------

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

OPENAI_API_KEY = (os.getenv("OPENAI_API_KEY") or "").strip()

RANDOM_SEED = 777
random.seed(RANDOM_SEED)

CONCURRENT_PROMPTS = 20  # LLM 프롬프트 동시 처리 개수

# 코호트 구성 (ENV 주입)
VIP_COUNT = int(os.getenv("VIP_COUNT", "100"))
LOYAL_COUNT = int(os.getenv("LOYAL_COUNT", "1000"))
REGULAR_COUNT = int(os.getenv("REGULAR_COUNT", "3000"))

def _parse_range(env_key: str, default: Tuple[int, int]) -> Tuple[int, int]:
    val = os.getenv(env_key)
    if val:
        try:
            a, b = [int(x.strip()) for x in val.split(",")]
            return (a, b)
        except Exception:
            pass
    return default

VIP_REVIEWS_RANGE = _parse_range("VIP_REVIEWS_RANGE", (80, 120))
LOYAL_REVIEWS_RANGE = _parse_range("LOYAL_REVIEWS_RANGE", (10, 30))
REGULAR_REVIEWS_RANGE = _parse_range("REGULAR_REVIEWS_RANGE", (1, 5))

# 방문 날짜 범위
DATE_START = datetime(2023, 1, 1)
DATE_END = datetime(2025, 7, 31)

# 파일 청크 크기(리뷰 건수)
CHUNK_SIZE = int(os.getenv("REVIEW_CHUNK_SIZE", "1000"))

# 리뷰 사진 개수 범위
REVIEW_PHOTO_MIN = 0
REVIEW_PHOTO_MAX = 3

# 프롬프트당 슬롯 패킹 기준
TARGET_SLOTS_PER_PROMPT = int(os.getenv("TARGET_SLOTS_PER_PROMPT", "20"))
MAX_SLOTS_PER_PROMPT = int(os.getenv("MAX_SLOTS_PER_PROMPT", "24"))


# ---- IO 유틸 -----------------------------------------------------------------

def read_json_array(path: Path) -> List[Dict]:
    """JSON 배열 로드(형식 검증)."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} is not a JSON array")
    return data


# ---- 날짜 생성(사용자 단위) ---------------------------------------------------

def gen_user_dates(n: int) -> List[str]:
    """
    한 사용자의 방문일 n개 생성.
    - 동일 날짜 금지
    - 인접일(±1일) 금지
    """
    if n <= 0:
        return []

    total_days = (DATE_END - DATE_START).days + 1
    used_days: set[int] = set()
    attempts = 0
    max_attempts = n * 200

    # 1차: 인접일 금지 규칙 최대 준수
    while len(used_days) < n and attempts < max_attempts:
        d = random.randint(0, total_days - 1)
        if any(abs(u - d) < 2 for u in used_days):
            attempts += 1
            continue
        used_days.add(d)
        attempts += 1

    # 2차: 남은 슬롯 보충(여전히 인접 금지)
    while len(used_days) < n and attempts < max_attempts * 2:
        d = random.randint(0, total_days - 1)
        if d not in used_days and (d - 1 not in used_days) and (d + 1 not in used_days):
            used_days.add(d)
        attempts += 1

    # 3차: 최후 보충(규칙 완화)
    while len(used_days) < n:
        d = random.randint(0, total_days - 1)
        if d not in used_days:
            used_days.add(d)

    dates = [(DATE_START + timedelta(days=dd)).strftime("%Y-%m-%d") for dd in sorted(used_days)]
    return dates[:n]


# ---- 로컬 리뷰 문장 생성(LLM 폴백) -------------------------------------------

POS_ADJ = ["깔끔하고", "신선하고", "풍미가 좋고", "친절하고", "분위기가 좋고", "재방문 의사 있고", "가격대도 괜찮고"]
MID_ADJ = ["무난하고", "평범하고", "아쉬운 점도 있지만", "분위기는 괜찮고"]
NEG_ADJ = ["간이 세고", "기대보다 밍밍하고", "서비스가 아쉽고", "재료가 부족하고"]


def fallback_review_text(name: str, desc: str) -> Tuple[str, float]:
    """간단한 감성 규칙 기반 리뷰/평점 생성."""
    r = random.random()
    if r < 0.6:
        adj = random.choice(POS_ADJ)
        rating = round(random.uniform(4.0, 5.0), 1)
        txt = f"{name} 다녀왔어요. {desc[:40]} {adj} 전반적으로 만족스러웠습니다."
    elif r < 0.9:
        adj = random.choice(MID_ADJ)
        rating = round(random.uniform(3.0, 4.0), 1)
        txt = f"{name} 방문 후기. {desc[:40]} {adj} 가볍게 식사하기 좋아요."
    else:
        adj = random.choice(NEG_ADJ)
        rating = round(random.uniform(2.0, 3.5), 1)
        txt = f"{name} 이용해봤는데 {adj} 기대에는 조금 못 미쳤어요."
    return (txt[:200], rating)


# ---- OpenAI 래퍼 -------------------------------------------------------------

class ReviewLLM:
    """OpenAI로 리뷰 텍스트/평점 생성(없으면 폴백)."""

    def __init__(self, api_key: Optional[str]):
        self.use_openai = OPENAI_AVAILABLE and bool(api_key)
        self.client = OpenAI(api_key=api_key) if self.use_openai else None

    async def generate_for_slots(self, slots: List[Dict]) -> List[Dict]:
        """
        slots: {slot_id, user_id, restaurant_id, name, description}
        반환: {slot_id, review_text, rating}
        """
        if not self.use_openai or not slots:
            return [
                {
                    "slot_id": s["slot_id"],
                    **dict(zip(("review_text", "rating"), fallback_review_text(s["name"], s["description"])))
                }
                for s in slots
            ]

        sys_msg = (
            "You write concise Korean restaurant reviews.\n"
            "- For each input item, output ONE JSON object per line with keys: slot_id, review_text, rating.\n"
            "- review_text: <= 500 Korean characters, natural tone, match the restaurant context.\n"
            "- rating: a float 0.0~5.0 (one decimal), consistent with sentiment.\n"
            "- Output JSON lines only. No extra commentary."
        )

        items = [
            {
                "slot_id": s["slot_id"],
                "restaurant_name": s["name"],
                "restaurant_description": s.get("description", ""),
            }
            for s in slots
        ]
        user_msg = "Write reviews for these items. JSON lines only:\n" + json.dumps(items, ensure_ascii=False)

        try:
            resp = await asyncio.to_thread(
                self.client.responses.create,
                model="gpt-5-nano",
                input=[{"role": "system", "content": sys_msg}, {"role": "user", "content": user_msg}],
                max_output_tokens=4096,
            )
            text = resp.output_text or ""
        except Exception:
            return [
                {
                    "slot_id": s["slot_id"],
                    **dict(zip(("review_text", "rating"), fallback_review_text(s["name"], s["description"])))
                }
                for s in slots
            ]

        results: Dict[int, Dict] = {}
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                sid = obj.get("slot_id")
                rtxt = obj.get("review_text")
                rat = obj.get("rating")
                if sid is not None and isinstance(rtxt, str) and isinstance(rat, (int, float)):
                    rat = max(0.0, min(5.0, float(rat)))
                    results[sid] = {"slot_id": sid, "review_text": rtxt[:200], "rating": round(rat, 1)}
            except Exception:
                continue

        out: List[Dict] = []
        for s in slots:
            if s["slot_id"] in results:
                out.append(results[s["slot_id"]])
            else:
                text_fb, rating_fb = fallback_review_text(s["name"], s["description"])
                out.append({"slot_id": s["slot_id"], "review_text": text_fb, "rating": rating_fb})
        return out


# ---- 코호트/보조 유틸 --------------------------------------------------------

def pick_cohorts(user_ids: List[int]) -> Tuple[List[int], List[int], List[int]]:
    """사용자 ID를 섞어 VIP/LOYAL/REGULAR로 분할."""
    users = user_ids[:]
    random.shuffle(users)
    vip = users[:VIP_COUNT]
    loyal = users[VIP_COUNT:VIP_COUNT + LOYAL_COUNT]
    regular = users[VIP_COUNT + LOYAL_COUNT:VIP_COUNT + LOYAL_COUNT + REGULAR_COUNT]
    return vip, loyal, regular


def choose_restaurants_for_user(n: int, restaurant_ids: List[int]) -> List[int]:
    """사용자가 방문할 식당 ID n개 샘플링(중복 없음)."""
    if n >= len(restaurant_ids):
        return random.sample(restaurant_ids, len(restaurant_ids))
    return random.sample(restaurant_ids, n)


def split_chunks(lst: List, k: int) -> List[List]:
    """리스트를 k개의 덩어리로 분할(마지막은 작을 수 있음)."""
    if k <= 1:
        return [lst]
    size = math.ceil(len(lst) / k)
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def pack_slots_by_target(slots: List[Dict], target: int = 20, hard_max: int = 24) -> List[List[Dict]]:
    """슬롯 리스트를 target 크기로 그리디 패킹(절대 hard_max 초과 금지)."""
    batches: List[List[Dict]] = []
    current: List[Dict] = []

    for slot in slots:
        current.append(slot)
        if len(current) >= target:
            batches.append(current[:hard_max])
            current = []
    if current:
        batches.append(current)
    return batches


# ---- 스트리밍 라이터(1000건 단위 파일 청크) ----------------------------------

class ReviewStreamer:
    """리뷰를 버퍼링하다가 1000건 단위로 파일에 기록하고 메모리 해제."""

    def __init__(self, data_dir: Path, chunk_size: int = 1000):
        self.data_dir = data_dir
        self.chunk_size = chunk_size
        self.buffer: List[Dict] = []
        self.next_review_id = 1

    def _build_review_photos(self, batch_reviews: List[Dict]) -> List[Dict]:
        """리뷰별 0~N장의 사진 레코드 생성."""
        photos: List[Dict] = []
        for rv in batch_reviews:
            n = random.randint(REVIEW_PHOTO_MIN, REVIEW_PHOTO_MAX)
            for idx in range(1, n + 1):
                photos.append(
                    {
                        "photo_id": None,
                        "review_id": rv["review_id"],
                        "image_url": f"/reviews/{rv['review_id']}/{idx}",
                        "is_deleted": False,
                        "created_at": rv["created_at"],
                        "updated_at": rv["updated_at"],
                    }
                )
        return photos

    def add_rows_and_maybe_flush(self, rows: List[Dict]) -> None:
        """리뷰 ID 부여 → 버퍼 적재 → chunk_size 이상이면 파일로 플러시."""
        for row in rows:
            row["review_id"] = self.next_review_id
            self.next_review_id += 1
            self.buffer.append(row)

        while len(self.buffer) >= self.chunk_size:
            chunk = self.buffer[: self.chunk_size]
            last_id = chunk[-1]["review_id"]

            out_review = self.data_dir / f"review_{last_id}.json"
            with out_review.open("w", encoding="utf-8") as f:
                json.dump(chunk, f, ensure_ascii=False, indent=2)

            photos = self._build_review_photos(chunk)
            out_photo = self.data_dir / f"review_photo_{last_id}.json"
            with out_photo.open("w", encoding="utf-8") as f:
                json.dump(photos, f, ensure_ascii=False, indent=2)

            print(f"[WRITE] {out_review.name} (rows={len(chunk)})")
            print(f"[WRITE] {out_photo.name} (rows={len(photos)})")

            del self.buffer[: self.chunk_size]

    def flush_remaining(self) -> None:
        """남은(<chunk_size) 버퍼를 마지막 파일로 기록."""
        if not self.buffer:
            return

        chunk = self.buffer[:]
        last_id = chunk[-1]["review_id"]

        out_review = self.data_dir / f"review_{last_id}.json"
        with out_review.open("w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, indent=2)

        photos = self._build_review_photos(chunk)
        out_photo = self.data_dir / f"review_photo_{last_id}.json"
        with out_photo.open("w", encoding="utf-8") as f:
            json.dump(photos, f, ensure_ascii=False, indent=2)

        print(f"[WRITE] {out_review.name} (rows={len(chunk)})")
        print(f"[WRITE] {out_photo.name} (rows={len(photos)})")

        self.buffer.clear()


# ---- 리뷰 생성 파이프라인 -----------------------------------------------------

async def run() -> None:
    """입력 로드 → 사용자별 방문 계획 → 슬롯 평탄화 → 20개 단위 패킹 → LLM/폴백 생성 → 스트리밍 저장."""
    restaurants = read_json_array(DATA_DIR / "restaurant.json")
    users = read_json_array(DATA_DIR / "user_account.json")

    if len(users) < VIP_COUNT + LOYAL_COUNT + REGULAR_COUNT:
        raise SystemExit("Not enough users for the requested cohort sizes.")

    rest_by_id = {r["restaurant_id"]: r for r in restaurants}
    restaurant_ids = list(rest_by_id.keys())
    user_ids = [u["user_id"] for u in users]

    vip_users, loyal_users, regular_users = pick_cohorts(user_ids)

    # 사용자별 (식당ID, 방문일) 계획
    user_plan: Dict[int, List[Tuple[int, str]]] = {}

    def assign_for(uid: int, n_min: int, n_max: int) -> None:
        n = random.randint(n_min, n_max)
        rids = choose_restaurants_for_user(n, restaurant_ids)
        dates = gen_user_dates(n)
        user_plan[uid] = list(zip(rids, dates))

    for uid in vip_users:
        assign_for(uid, *VIP_REVIEWS_RANGE)

    for uid in loyal_users:
        assign_for(uid, *LOYAL_REVIEWS_RANGE)

    for uid in regular_users:
        assign_for(uid, *REGULAR_REVIEWS_RANGE)

    # 슬롯 평탄화
    all_slots: List[Dict] = []
    slot_seq = 1
    # 인터리브: VIP → LOYAL → REGULAR 라운드로빈(편향 완화)
    buckets = [vip_users, loyal_users, regular_users]
    max_len = max(len(b) for b in buckets)
    for i in range(max_len):
        for bucket in buckets:
            if i < len(bucket):
                uid = bucket[i]
                for rid, vdate in user_plan[uid]:
                    r = rest_by_id[rid]
                    all_slots.append(
                        {
                            "slot_id": slot_seq,
                            "user_id": uid,
                            "restaurant_id": rid,
                            "name": r["name"],
                            "description": r.get("description") or "",
                            "visited_at": vdate,
                        }
                    )
                    slot_seq += 1

    # 20개 단위 패킹(ENV로 조정 가능)
    batches = pack_slots_by_target(all_slots, TARGET_SLOTS_PER_PROMPT, MAX_SLOTS_PER_PROMPT)
    prompt_tasks: List[Tuple[str, List[Dict]]] = [("MIXED", batch) for batch in batches]

    # 스트리밍 저장
    llm = ReviewLLM(OPENAI_API_KEY)
    streamer = ReviewStreamer(DATA_DIR, CHUNK_SIZE)

    async def run_prompt(kind: str, slots: List[Dict]) -> List[Dict]:
        """슬롯 묶음 → 리뷰텍스트/평점 생성 → 저장 레코드로 변환."""
        results = await llm.generate_for_slots(slots)
        results_map = {r["slot_id"]: r for r in results}
        nowts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows: List[Dict] = []
        for s in slots:
            rr = results_map.get(s["slot_id"])
            if rr:
                rating = rr["rating"]
                rtxt = (rr["review_text"] or "")[:200]
            else:
                text_fb, rating = fallback_review_text(s["name"], s["description"])
                rtxt = text_fb[:200]

            rows.append(
                {
                    "user_id": s["user_id"],
                    "restaurant_id": s["restaurant_id"],
                    "rating": rating,
                    "review_text": rtxt,
                    "visited_at": s["visited_at"],
                    "is_deleted": False,
                    "created_at": nowts,
                    "updated_at": nowts,
                }
            )
        return rows

    # 실행(동시 처리 → 스트리밍 플러시)
    total_tasks = len(prompt_tasks)
    for start in range(0, total_tasks, CONCURRENT_PROMPTS):
        group = prompt_tasks[start : start + CONCURRENT_PROMPTS]
        results_lists = await asyncio.gather(*[run_prompt(kind, slots) for (kind, slots) in group])
        for idx, rows in enumerate(results_lists):
            streamer.add_rows_and_maybe_flush(rows)
            gidx = start + idx + 1
            print(f"[진행] Prompt {gidx}/{total_tasks} 완료 (rows={len(rows)})")

    streamer.flush_remaining()
    print("[OK] review generation done.")


# ---- 엔트리포인트 ------------------------------------------------------------

def main() -> None:
    if not OPENAI_AVAILABLE or not OPENAI_API_KEY:
        print("[WARN] OPENAI_API_KEY missing or SDK unavailable. Falling back to local review texts.")
    asyncio.run(run())


if __name__ == "__main__":
    main()
