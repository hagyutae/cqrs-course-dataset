"""
Postgres 로 사용자 JSON 적재 스크립트

핵심
- .env: DATABASE_URL 필수, DATA_DIR(기본 ./data), LOAD_TRUNCATE(optional)
- 입력: user_account.json → user_account, user_profile.json → user_profile
- 기본 동작: ON CONFLICT DO NOTHING (upsert-safe)
- LOAD_TRUNCATE=true 일 때 적재 전 TRUNCATE
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
    "user_account": DATA_DIR / "user_account.json",
    "user_profile": DATA_DIR / "user_profile.json",
}


# ---- 유틸 --------------------------------------------------------------------

def read_json_array(path: Path) -> List[Dict]:
    """JSON 배열 파일 로드(없으면 빈 리스트)."""
    if not path.exists():
        print(f"[WARN] File not found: {path}")
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"JSON at {path} is not an array.")
    return data


def execute_values(conn, sql: str, rows: Sequence[Tuple], page_size: int = 5000) -> int:
    """대량 INSERT (psycopg2.extras.execute_values)."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        extras.execute_values(cur, sql, rows, page_size=page_size)
    return len(rows)


def truncate_tables(conn, table_names: List[str]) -> None:
    """의존 고려해 순서대로 TRUNCATE."""
    with conn.cursor() as cur:
        for t in table_names:
            cur.execute(f"TRUNCATE TABLE {t} RESTART IDENTITY CASCADE;")
    conn.commit()
    print(f"[OK] Truncated: {', '.join(table_names)}")


# ---- 로더(테이블별) -----------------------------------------------------------

def load_user_account(conn, rows_json: List[Dict]) -> int:
    """user_account 테이블 적재."""
    rows = [
        (
            d["user_id"],
            d["username"],
            d["password_hash"],
            d.get("email"),
            d.get("phone_number"),
            d.get("joined_at"),
            bool(d.get("is_deleted", False)),
            d.get("created_at"),
            d.get("updated_at"),
        )
        for d in rows_json
    ]
    sql = """
          INSERT INTO user_account (
              user_id, username, password_hash, email, phone_number,
              joined_at, is_deleted, created_at, updated_at
          )
          VALUES %s
              ON CONFLICT (user_id) DO NOTHING; \
          """
    return execute_values(conn, sql, rows)


def load_user_profile(conn, rows_json: List[Dict]) -> int:
    """user_profile 테이블 적재."""
    rows = [
        (
            d["user_id"],
            d["nickname"],
            d.get("image_path"),
            d.get("bio"),
            bool(d.get("is_deleted", False)),
            d.get("created_at"),
            d.get("updated_at"),
        )
        for d in rows_json
    ]
    sql = """
          INSERT INTO user_profile (
              user_id, nickname, image_path, bio,
              is_deleted, created_at, updated_at
          )
          VALUES %s
              ON CONFLICT (user_id) DO NOTHING; \
          """
    return execute_values(conn, sql, rows)


# ---- 엔트리포인트 ------------------------------------------------------------

def main() -> None:
    """파일 로드 → (옵션) TRUNCATE → 테이블별 적재."""
    ua = read_json_array(FILES["user_account"])
    up = read_json_array(FILES["user_profile"])

    if not ua and not up:
        raise SystemExit(f"ERROR: No input data found under {DATA_DIR}")

    with psycopg2.connect(DATABASE_URL) as conn:
        conn.autocommit = False

        if LOAD_TRUNCATE:
            # 프로필이 계정을 참조한다고 가정하여 먼저 프로필부터 비움
            truncate_tables(conn, ["user_profile", "user_account"])

        inserted = 0

        if ua:
            print(f"[LOAD] user_account ... ({len(ua)} rows)")
            inserted += load_user_account(conn, ua)
            conn.commit()

        if up:
            print(f"[LOAD] user_profile ... ({len(up)} rows)")
            inserted += load_user_profile(conn, up)
            conn.commit()

        print(f"[DONE] inserted rows (sum of batches): {inserted}")
        print(f"[INFO] Data dir: {DATA_DIR}")


if __name__ == "__main__":
    main()
