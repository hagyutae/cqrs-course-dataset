# CQRS Course Dataset

"**음식점 및 리뷰 검색 서비스**" 예제를 위한 합성 데이터와 적재 스크립트를 제공하는 저장소입니다.  
사용자·음식점·리뷰 데이터를 JSON으로 생성하고 PostgreSQL에 적재하는 전체 흐름을 포함합니다.

---

## 구성

- **`initdb/ddl.sql`** – 카테고리, 음식점, 사용자, 리뷰 등 전체 테이블 DDL 및 초기 카테고리 데이터
- **`data/`** – 생성된 JSON 데이터 (`restaurant.json`, `user_account.json`, `review_*.json` 등)
- **`synthetic_*.py`** – 합성 데이터 생성 스크립트  
  - `synthetic_users.py`: 사용자 계정/프로필 생성
  - `synthetic_restaurants.py`: 음식점·위치·카테고리·이미지 생성
  - `synthetic_reviews.py`: 리뷰·리뷰사진 청크 파일 생성
- **`load_*.py`** – 생성된 JSON을 PostgreSQL에 적재  
  - `load_users.py`
  - `load_restaurants.py`
  - `load_reviews.py`
- **`docker-compose.yml`** – Postgres 17 컨테이너 구성 파일

---

## 환경 준비 (Poetry)

1. **Python 3.13+ 및 [Poetry](https://python-poetry.org) 설치**
   ```bash
   poetry install
   ```
   `poetry install`은 가상 환경을 만들고 필요한 패키지(`dotenv`, `openai`, `psycopg2-binary` 등)를 설치합니다.

2. **PostgreSQL 실행**
   ```bash
   docker compose up -d
   ```
   `docker-compose.yml`은 `restaurant` DB를 갖춘 Postgres 17 컨테이너를 기동합니다.

3. **환경 변수 설정**
   - `.env.sample`을 복사해 `.env` 파일을 생성한 뒤 값을 수정합니다.
   - 예:
     ```bash
     cp .env.sample .env
     ```

### `.env`에 지정하는 변수들

| 변수명 | 의미 |
|-------|------|
| `OPENAI_API_KEY` | OpenAI를 사용한 이름·설명·리뷰 텍스트 생성을 위한 API Key |
| `DATABASE_URL` | PostgreSQL 접속 URL (`postgresql://user:pw@host:port/db`) |
| `DATA_DIR` | 생성된 JSON 파일을 저장/읽을 디렉토리 경로 |
| `NUM_RESTAURANTS` | 생성할 음식점 개수 |
| `USER_COUNT` | 생성할 전체 사용자 수 |
| `VIP_COUNT`, `LOYAL_COUNT`, `REGULAR_COUNT` | 리뷰 생성 시 각 사용자 코호트(VIP/충성/일반) 규모 |
| `VIP_REVIEWS_RANGE`, `LOYAL_REVIEWS_RANGE`, `REGULAR_REVIEWS_RANGE` | 각 코호트별 사용자당 리뷰 개수 범위 (min,max) |
| `REVIEW_CHUNK_SIZE` | 리뷰 JSON 청크 파일당 포함할 리뷰 수 |
| `TARGET_SLOTS_PER_PROMPT`, `MAX_SLOTS_PER_PROMPT` | LLM 프롬프트당 리뷰 슬롯 패킹 기준과 상한 |

---

## 데이터 생성

> 모든 생성 스크립트는 기본적으로 `DATA_DIR=./data`에 결과 파일을 생성합니다.  
> 명령은 `poetry run`으로 실행합니다.

### 1. 사용자
```bash
poetry run python synthetic_users.py
```
- `USER_COUNT`로 생성할 사용자 수 지정 (기본 10000)

### 2. 음식점
```bash
poetry run python synthetic_restaurants.py
```
- `OPENAI_API_KEY` 설정 시 OpenAI로 이름·설명·카테고리를 생성하고, 없으면 로컬 규칙으로 대체
- `NUM_RESTAURANTS`로 음식점 수 조절 (기본 2500)

### 3. 리뷰
```bash
poetry run python synthetic_reviews.py
```
- `review_*.json`과 `review_photo_*.json`이 `REVIEW_CHUNK_SIZE` 단위로 청크 생성
- 사용자 코호트 비율(`VIP_COUNT` 등)과 리뷰 수 범위를 환경 변수로 조절

---

## 데이터 적재

> 모든 적재 스크립트는 `DATABASE_URL`과 `DATA_DIR`을 사용하며, `LOAD_TRUNCATE=true`로 지정하면 적재 전에 대상 테이블을 비웁니다.

### 1. 사용자
```bash
poetry run python load_users.py
```
- 입력: `user_account.json`, `user_profile.json`

### 2. 음식점
```bash
poetry run python load_restaurants.py
```
- 입력: `restaurant*.json` (음식점·위치·이미지·카테고리)

### 3. 리뷰
```bash
poetry run python load_reviews.py
```
- 입력: `review_*.json`, `review_photo_*.json` 청크 파일
- 추가 환경변수: `REBUILD_STATS` (리뷰 통계 재계산 여부)

---

## 테이블 개요

- `category`
- `restaurant`, `restaurant_location`, `restaurant_image`, `restaurant_category`
- `user_account`, `user_profile`
- `review`, `review_photo`, `restaurant_review_stats`

---

## 미리 생성된 데이터 다운로드

GitHub **Releases**에서 `Data.zip - YYMMDD` 형식의 릴리스를 확인하면, 해당 릴리스에 포함된 **data.zip** 파일을 다운로드하여 데이터만 바로 사용할 수 있습니다. 로컬에서 합성 스크립트를 실행하지 않고도 준비된 JSON 데이터를 얻을 수 있습니다.

---

## 전체 사용 흐름 예시

```bash
# 1) 데이터 생성
poetry run python synthetic_users.py
poetry run python synthetic_restaurants.py
poetry run python synthetic_reviews.py

# 2) DB 적재
poetry run python load_users.py
poetry run python load_restaurants.py
poetry run python load_reviews.py
```

이 저장소의 데이터와 스크립트는 CQRS 강의 및 실습용으로 자유롭게 활용할 수 있습니다.
