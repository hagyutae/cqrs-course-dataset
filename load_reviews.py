#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Postgres로 리뷰/사진 JSON 적재 스크립트

핵심
- 환경변수:
  - DATABASE_URL (필수)
  - DATA_DIR (기본: ./data)
  - LOAD_TRUNCATE (기본: false)  → true면 적재 전 TRUNCATE
  - REBUILD_STATS (기본: true)   → true면 적재 후 restaurant_review_stats 재계산
- 대상 테이블:
  - review(review_id, user_id, restaurant_id, rating, review_text, visited_at, is_deleted, created_at, updated_at)
  - review_photo(photo_id SERIAL, review_id, image_url, is_deleted, created_at, updated_at)
  - restaurant_review_stats(restaurant_id, review_count, avg_rating, updated_at)
"""

from __future__ import annotations

import os
import re
import json
from pathlib import Path
from typing import List, Dict, Iterable, Tuple, Sequence

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras as extras


# ---- 설정 --------------------------------------------------------------------

load_dotenv()

DATABASE_URL: str = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise SystemExit("ERROR: DATABASE_URL not set")

DATA_DIR: Path = Path(os.getenv("DATA_DIR", "./data")).resolve()
LOAD_TRUNCATE: bool = os.getenv("LOAD_TRUNCATE", "false").lower() in ("1", "true", "yes")
REBUILD_STATS: bool = os.getenv("REBUILD_STATS", "true").lower() in ("1", "true", "yes")

BATCH_PAGE_SIZE: int = 5000  # execute_values page size


# ---- 유틸 --------------------------------------------------------------------

def list_chunk_files(prefix: str) -> List[Path]:
    """prefix(review|review_photo)에 해당하는 청크 파일 목록을 숫자 접미순으로 정렬해 반환."""
    patt = re.compile(rf"^{re.escape(prefix)}_(\d+)\.json$")
    items: List[Tuple[int, Path]] = []
    for p in DATA_DIR.glob(f"{prefix}_*.json"):
        m = patt.match(p.name)
        if m:
            items.append((int(m.group(1)), p))
    items.sort(key=lambda x: x[0])
    return [p for _, p in items]


def read_json_array(path: Path) -> List[Dict]:
    """JSON 배열 파일 로드(형식 검증)."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{path} is not a JSON array")
    return data


def execute_values(conn, sql: str, rows: Sequence[Tuple], page_size: int = BATCH_PAGE_SIZE) -> int:
    """psycopg2.extras.execute_values로 대량 INSERT."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        extras.execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)


def truncate_tables(conn, table_names: List[str]) -> None:
    """지정 테이블을 순서대로 TRUNCATE."""
    with conn.cursor() as cur:
        for t in table_names:
            cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE;")
    conn.commit()
    print(f"[OK] Truncated: {', '.join(table_names)}")


# ---- 로더(파일 단위) ---------------------------------------------------------

def load_review_file(conn, path: Path) -> int:
    """review_*.json 한 파일을 review 테이블에 적재."""
    data = read_json_array(path)
    rows = [
        (
            int(d["review_id"]),
            int(d["user_id"]),
            int(d["restaurant_id"]),
            float(d["rating"]),
            d.get("review_text"),
            d.get("visited_at"),
            bool(d.get("is_deleted", False)),
            d.get("created_at"),
            d.get("updated_at"),
        )
        for d in data
    ]
    sql = """
          INSERT INTO review (
              review_id, user_id, restaurant_id, rating, review_text,
              visited_at, is_deleted, created_at, updated_at
          )
          VALUES %s
              ON CONFLICT (review_id) DO NOTHING; \
          """
    n = execute_values(conn, sql, rows)
    conn.commit()
    print(f"[LOAD] {path.name} -> review (+{n})")
    return n


def load_review_photo_file(conn, path: Path) -> int:
    """review_photo_*.json 한 파일을 review_photo 테이블에 적재 (photo_id는 SERIAL)."""
    data = read_json_array(path)
    rows = [
        (
            int(d["review_id"]),
            d["image_url"],
            bool(d.get("is_deleted", False)),
            d.get("created_at"),
            d.get("updated_at"),
        )
        for d in data
    ]
    sql = """
          INSERT INTO review_photo (
              review_id, image_url, is_deleted, created_at, updated_at
          )
          VALUES %s; \
          """
    n = execute_values(conn, sql, rows)
    conn.commit()
    print(f"[LOAD] {path.name} -> review_photo (+{n})")
    return n


# ---- 통계 재계산(선택) --------------------------------------------------------

def rebuild_restaurant_review_stats(conn) -> None:
    """삭제되지 않은 리뷰 기준으로 집계 테이블(restaurant_review_stats)을 재계산/업서트."""
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH agg AS (
                SELECT
                    restaurant_id,
                    COUNT(*)::int AS review_count,
                    ROUND(AVG(rating)::numeric, 1) AS avg_rating
                FROM review
                WHERE is_deleted = FALSE
                GROUP BY restaurant_id
            )
            INSERT INTO restaurant_review_stats (restaurant_id, review_count, avg_rating, updated_at)
            SELECT a.restaurant_id, a.review_count, COALESCE(a.avg_rating, 0.0), NOW()
            FROM agg a
                ON CONFLICT (restaurant_id) DO UPDATE
                   SET review_count = EXCLUDED.review_count,
                       avg_rating   = EXCLUDED.avg_rating,
                       updated_at   = NOW();
            """
        )
    conn.commit()
    print("[OK] Rebuilt restaurant_review_stats")


# ---- 엔트리포인트 ------------------------------------------------------------

def main() -> None:
    """리뷰/사진 청크 파일 탐색 → (옵션) TRUNCATE → 순차 적재 → (옵션) 통계 재계산."""
    review_files = list_chunk_files("review")
    photo_files = list_chunk_files("review_photo")

    if not review_files:
        raise SystemExit(f"ERROR: No review_*.json files under {DATA_DIR}")

    with psycopg2.connect(DATABASE_URL) as conn:
        conn.autocommit = False

        if LOAD_TRUNCATE:
            # 사진 → 리뷰 순으로 비움(의존 역순)
            truncate_tables(conn, ["review_photo", "review"])

        inserted_reviews = 0
        inserted_photos = 0

        # 리뷰 먼저 로드(숫자 접미 오름차순)
        for rf in review_files:
            inserted_reviews += load_review_file(conn, rf)

        # 사진 로드(FK 없어도 안전하지만 순서를 지킴)
        for pf in photo_files:
            inserted_photos += load_review_photo_file(conn, pf)

        print(f"[DONE] reviews inserted: {inserted_reviews}")
        print(f"[DONE] review_photos inserted: {inserted_photos}")

        if REBUILD_STATS:
            rebuild_restaurant_review_stats(conn)

        print(f"[INFO] Data dir: {DATA_DIR}")


if __name__ == "__main__":
    main()
