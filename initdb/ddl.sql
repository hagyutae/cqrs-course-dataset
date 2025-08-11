-- 1) 카테고리
CREATE TABLE category (
    category_id   BIGSERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL UNIQUE,   -- 예: 중식, 일식, 퓨전한식
    is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 2) 음식점
CREATE TABLE restaurant (
    restaurant_id BIGSERIAL PRIMARY KEY,
    name          VARCHAR(150) NOT NULL,          -- 음식점 이름
    description   TEXT,                           -- 음식점 소개
    phone_number  VARCHAR(30),                    -- 대표전화번호
    opening_hours TEXT,                           -- 영업시간 문자열
    is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 3) 음식점-카테고리 연결 (N:N)
CREATE TABLE restaurant_category (
    rc_id         BIGSERIAL PRIMARY KEY,
    restaurant_id BIGINT NOT NULL,
    category_id   BIGINT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 4) 음식점 위치/주소
CREATE TABLE restaurant_location (
    restaurant_id BIGINT PRIMARY KEY,             -- 1:1 매핑
    latitude      DOUBLE PRECISION NOT NULL,
    longitude     DOUBLE PRECISION NOT NULL,
    address_line  VARCHAR(255) NOT NULL,          -- 전체 주소
    region_si_do  VARCHAR(50) NOT NULL,           -- 시/도
    region_si_gun_gu VARCHAR(50) NOT NULL,        -- 시/군/구
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 5) 음식점 이미지 (여러 장)
CREATE TABLE restaurant_image (
    image_id      BIGSERIAL PRIMARY KEY,
    restaurant_id BIGINT NOT NULL,
    image_path    TEXT NOT NULL,                  -- 이미지 URL
    is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    index         INTEGER NOT NULL DEFAULT 0,     -- 이미지 순서
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 6) 사용자 계정
CREATE TABLE user_account (
    user_id       BIGSERIAL PRIMARY KEY,
    username      VARCHAR(50) NOT NULL UNIQUE,    -- 로그인 ID
    password_hash TEXT NOT NULL,                  -- 비밀번호 해시
    email         VARCHAR(120),
    phone_number  VARCHAR(30),
    joined_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 7) 사용자 프로필
CREATE TABLE user_profile (
    user_id       BIGINT PRIMARY KEY,
    nickname      VARCHAR(50) NOT NULL,
    image_path    TEXT,
    bio           VARCHAR(160),
    is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 8) 리뷰
CREATE TABLE review (
    review_id     BIGSERIAL PRIMARY KEY,
    user_id       BIGINT NOT NULL,
    restaurant_id BIGINT NOT NULL,
    rating        NUMERIC(2,1) NOT NULL,
    review_text   TEXT,
    visited_at    DATE,
    is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 9) 리뷰 사진
CREATE TABLE review_photo (
    photo_id      BIGSERIAL PRIMARY KEY,
    review_id     BIGINT NOT NULL,
    image_url     TEXT NOT NULL,
    is_deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- 10) 음식점 리뷰 통계
CREATE TABLE restaurant_review_stats (
    restaurant_id BIGINT PRIMARY KEY,
    review_count  INTEGER NOT NULL DEFAULT 0,
    avg_rating    NUMERIC(2,1) NOT NULL DEFAULT 0.0,
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO category (name) VALUES
('한식'),
('중식'),
('일식'),
('양식'),
('아시아음식'),
('카페/디저트'),
('패스트푸드'),
('치킨'),
('피자'),
('주점/술집'),
('기타');
