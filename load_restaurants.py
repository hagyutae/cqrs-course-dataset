"""
Postgres 로 JSON 데이터 적재 스크립트

핵심
- .env의 DATABASE_URL 필수, DATA_DIR(기본 ./data)
- 테이블: restaurant, restaurant_location, restaurant_image, restaurant_category
- 기본 동작: ON CONFLICT DO NOTHING (upsert-safe)
- LOAD_TRUNCATE=true 설정 시 적재 전 TRUNCATE 수행
"""

from __future__ import annotations

import os
import json
from pathlib import Path
from typing import List, Dict, Sequence, Tuple

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras as extras


# ---- 설정 --------------------------------------------------------------------

load_dotenv()

DATABASE_URL: str = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise SystemExit("ERROR: DATABASE_URL not set in .env")

DATA_DIR: Path = Path(os.getenv("DATA_DIR", "./data")).resolve()
LOAD_TRUNCATE: bool = os.getenv("LOAD_TRUNCATE", "false").lower() in ("1", "true", "yes")

FILES: Dict[str, Path] = {
    "restaurant": DATA_DIR / "restaurant.json",
    "restaurant_location": DATA_DIR / "restaurant_location.json",
    "restaurant_image": DATA_DIR / "restaurant_image.json",
    "restaurant_category": DATA_DIR / "restaurant_category.json",
}


# ---- 유틸 --------------------------------------------------------------------

def read_json_array(path: Path) -> List[Dict]:
    """JSON 배열 파일을 로드 (없으면 빈 리스트 반환)."""
    if not path.exists():
        print(f"[WARN] File not found: {path}")
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"JSON at {path} is not an array.")
    return data


def execute_values(conn, sql: str, rows: Sequence[Tuple], page_size: int = 5000) -> int:
    """대량 INSERT 수행 (execute_values)."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        extras.execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)


def truncate_tables(conn, table_names: List[str]) -> None:
    """의존성 고려해 순서대로 TRUNCATE."""
    with conn.cursor() as cur:
        for t in table_names:
            cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE;")
    conn.commit()
    print(f"[OK] Truncated: {', '.join(table_names)}")


# ---- 로더(테이블별) -----------------------------------------------------------

def load_restaurant(conn, data: List[Dict]) -> int:
    """restaurant 테이블 적재."""
    rows = [
        (
            d["restaurant_id"],
            d["name"],
            d.get("description"),
            d.get("phone_number"),
            d.get("opening_hours"),
            bool(d.get("is_deleted", False)),
            d.get("created_at"),
            d.get("updated_at"),
        )
        for d in data
    ]
    sql = """
          INSERT INTO restaurant (
              restaurant_id, name, description, phone_number, opening_hours,
              is_deleted, created_at, updated_at
          )
          VALUES %s
              ON CONFLICT (restaurant_id) DO NOTHING; \
          """
    return execute_values(conn, sql, rows)


def load_restaurant_location(conn, data: List[Dict]) -> int:
    """restaurant_location 테이블 적재 (1:1)."""
    rows = [
        (
            d["restaurant_id"],
            float(d["latitude"]),
            float(d["longitude"]),
            d["address_line"],
            d["region_si_do"],
            d["region_si_gun_gu"],
            d.get("created_at"),
            d.get("updated_at"),
        )
        for d in data
    ]
    sql = """
          INSERT INTO restaurant_location (
              restaurant_id, latitude, longitude, address_line,
              region_si_do, region_si_gun_gu, created_at, updated_at
          )
          VALUES %s
              ON CONFLICT (restaurant_id) DO NOTHING; \
          """
    return execute_values(conn, sql, rows)


def load_restaurant_image(conn, data: List[Dict]) -> int:
    """restaurant_image 테이블 적재 (N:1)."""
    rows = [
        (
            d["image_id"],
            d["restaurant_id"],
            d["image_path"],
            bool(d.get("is_deleted", False)),
            int(d.get("index", 0)),
            d.get("created_at"),
            d.get("updated_at"),
        )
        for d in data
    ]
    sql = """
          INSERT INTO restaurant_image (
              image_id, restaurant_id, image_path, is_deleted, index, created_at, updated_at
          )
          VALUES %s
              ON CONFLICT (image_id) DO NOTHING; \
          """
    return execute_values(conn, sql, rows)


def load_restaurant_category(conn, data: List[Dict]) -> int:
    """restaurant_category 테이블 적재 (매핑)."""
    rows = [
        (
            d["rc_id"],
            d["restaurant_id"],
            d["category_id"],
            d.get("created_at"),
        )
        for d in data
    ]
    sql = """
          INSERT INTO restaurant_category (
              rc_id, restaurant_id, category_id, created_at
          )
          VALUES %s
              ON CONFLICT (rc_id) DO NOTHING; \
          """
    return execute_values(conn, sql, rows)


# ---- 엔트리포인트 ------------------------------------------------------------

def main() -> None:
    """파일 로드 → (옵션) TRUNCATE → 테이블별 적재."""
    rest = read_json_array(FILES["restaurant"])
    loc = read_json_array(FILES["restaurant_location"])
    imgs = read_json_array(FILES["restaurant_image"])
    rc = read_json_array(FILES["restaurant_category"])

    if not any([rest, loc, imgs, rc]):
        raise SystemExit(f"ERROR: No input data found under {DATA_DIR}")

    with psycopg2.connect(DATABASE_URL) as conn:
        conn.autocommit = False

        if LOAD_TRUNCATE:
            # 의존성 역순으로 자식→부모 순서 TRUNCATE
            truncate_tables(conn, [
                "restaurant_category",
                "restaurant_image",
                "restaurant_location",
                "restaurant",
            ])

        total = 0

        print(f"[LOAD] restaurant ... ({len(rest)} rows)")
        total += load_restaurant(conn, rest)
        conn.commit()

        print(f"[LOAD] restaurant_location ... ({len(loc)} rows)")
        total += load_restaurant_location(conn, loc)
        conn.commit()

        print(f"[LOAD] restaurant_image ... ({len(imgs)} rows)")
        total += load_restaurant_image(conn, imgs)
        conn.commit()

        print(f"[LOAD] restaurant_category ... ({len(rc)} rows)")
        total += load_restaurant_category(conn, rc)
        conn.commit()

        print(f"[DONE] inserted rows (sum of batches): {total}")
        print(f"[INFO] Data dir: {DATA_DIR}")


if __name__ == "__main__":
    main()
