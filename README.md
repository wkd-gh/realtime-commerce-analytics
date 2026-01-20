# Real-time Multi-Platform Commerce Analytics Pipeline

실시간 멀티 플랫폼 커머스 분석 파이프라인 - YouTube, Twitter, TikTok, Naver Shopping, Google Trends 데이터를 수집하고 Kafka + Spark Streaming으로 처리하여 미디어커머스 인사이트를 도출합니다.

## 시스템 아키텍처

```
[Multi-Platform APIs]
  ├─ YouTube API (비디오, 댓글, 채널)
  ├─ Twitter API (트윗, 해시태그)
  ├─ TikTok API (비디오, 트렌드)
  ├─ Naver Shopping API (상품, 가격, 리뷰)
  └─ Google Trends (검색 트렌드)
       ↓
[Producer Services] → [Apache Kafka Cluster]
       ↓
[Apache Spark Structured Streaming]
  ├─ Data Parsing & Enrichment
  ├─ Windowed Aggregations (1m, 5m, 15m, 1h)
  ├─ Sentiment Analysis (Korean BERT)
  ├─ Trend Detection (Z-score)
  └─ Cross-Platform Analysis
       ↓
[PostgreSQL] [ClickHouse] [Redis] [Elasticsearch]
       ↓
[Grafana] [Apache Superset] [Slack Notifications]
```

## 기술 스택

- **수집**: YouTube API, Twitter API, TikTok API, Naver Shopping API, Google Trends
- **스트리밍**: Apache Kafka 3.6+, Zookeeper
- **처리**: Apache Spark 3.5+, PySpark Structured Streaming
- **저장**: PostgreSQL 16, ClickHouse, Redis 7, Elasticsearch 8
- **ML**: Transformers (klue/bert-base), KoNLPy
- **시각화**: Apache Superset 3.0+, Grafana 10+
- **알림**: Slack Webhook
- **인프라**: Docker & Docker Compose

## 빠른 시작

### 1. 요구사항

- Docker & Docker Compose
- Python 3.10+
- 최소 8GB RAM

### 2. 설정

```bash
# 저장소 클론
git clone <repository-url>
cd realtime-commerce-analytics

# 환경 설정
cp .env.example .env
# .env 파일에 API 키 설정

# 의존성 설치
pip install -r requirements.txt
```

### 3. 실행

```bash
# 전체 인프라 시작
make start

# 또는 수동으로
docker-compose up -d

# Kafka 토픽 생성
make topics

# 데이터베이스 초기화
make db-init

# 프로듀서 시작
make producers

# Spark Streaming 시작
make streaming
```

### 4. 접속

- **Kafka UI**: http://localhost:8080
- **Grafana**: http://localhost:3000 (admin/admin)
- **Superset**: http://localhost:8088 (admin/admin)
- **Spark UI**: http://localhost:8081
- **Prometheus**: http://localhost:9090

## 프로젝트 구조

```
realtime-commerce-analytics/
├── docker-compose.yml          # 전체 인프라 정의
├── .env.example                # 환경 변수 템플릿
├── requirements.txt            # Python 의존성
├── Makefile                    # 편의 명령어
│
├── config/                     # 설정 파일
│   ├── kafka/                  # Kafka 설정
│   ├── spark/                  # Spark 설정
│   ├── grafana/                # Grafana 대시보드
│   ├── superset/               # Superset 설정
│   └── platforms.yaml          # 플랫폼 API 설정
│
├── producers/                  # 데이터 수집 프로듀서
│   ├── base_producer.py        # 추상 기본 클래스
│   ├── youtube_producer.py     # YouTube 수집
│   ├── twitter_producer.py     # Twitter 수집
│   ├── tiktok_producer.py      # TikTok 수집
│   ├── naver_shopping_producer.py  # Naver 수집
│   ├── google_trends_producer.py   # Google Trends 수집
│   └── scheduler.py            # APScheduler 스케줄러
│
├── streaming/                  # Spark Streaming
│   ├── main.py                 # 메인 애플리케이션
│   ├── schemas/                # 데이터 스키마
│   ├── transformations/        # 변환 로직
│   ├── ml/                     # ML 컴포넌트
│   └── sinks/                  # 출력 싱크
│
├── database/                   # 데이터베이스
│   ├── migrations/             # PostgreSQL 마이그레이션
│   └── clickhouse/             # ClickHouse 스키마
│
├── scripts/                    # 운영 스크립트
└── tests/                      # 테스트
```

## 주요 기능

### 1. 멀티 플랫폼 데이터 수집
- YouTube: 비디오 메타데이터, 댓글, 채널 정보
- Twitter: 트윗, 해시태그, 멘션
- TikTok: 바이럴 비디오, 해시태그 트렌드
- Naver Shopping: 상품 정보, 가격 변동
- Google Trends: 검색 트렌드, 관심도

### 2. 실시간 스트림 처리
- 윈도우 기반 집계 (1분, 5분, 15분, 1시간)
- 플랫폼별 참여율 계산
- 성장률 및 변화 추적

### 3. ML 기반 분석
- 한국어 감성 분석 (klue/bert-base)
- Z-score 기반 트렌드 탐지
- TF-IDF 키워드 추출

### 4. 교차 플랫폼 분석
- 브랜드별 멀티 플랫폼 성과 비교
- 플랫폼 간 상관관계 분석
- 바즈 스코어 계산

### 5. 실시간 알림
- Slack 급상승 콘텐츠 알림
- 이상 패턴 탐지 알림
- 일일 리포트

## API 설정

### YouTube Data API v3
1. [Google Cloud Console](https://console.cloud.google.com/) 접속
2. 새 프로젝트 생성
3. YouTube Data API v3 활성화
4. API 키 생성
5. `.env`에 `YOUTUBE_API_KEY=your_key` 추가

### Twitter API v2
1. [Twitter Developer Portal](https://developer.twitter.com/) 접속
2. 앱 생성 및 Bearer Token 발급
3. `.env`에 `TWITTER_BEARER_TOKEN=your_token` 추가

### Naver Shopping API
1. [Naver Developers](https://developers.naver.com/) 접속
2. 애플리케이션 등록
3. `.env`에 Client ID/Secret 추가

## 명령어 참조

```bash
# 인프라
make start          # 전체 서비스 시작
make stop           # 전체 서비스 중지
make logs           # 로그 확인
make health         # 헬스 체크

# 개발
make test           # 테스트 실행
make lint           # 린터 실행

# 프로듀서
make producers      # 모든 프로듀서 시작
make producer-youtube  # YouTube 프로듀서만

# 스트리밍
make streaming      # Spark Streaming 시작

# 데이터베이스
make db-init        # DB 초기화
make db-migrate     # 마이그레이션 실행

# Kafka
make topics         # 토픽 생성
make topics-list    # 토픽 목록
```

## 모니터링

### Grafana 대시보드
- System Monitoring: Kafka, Spark, DB 메트릭
- Platform Metrics: 플랫폼별 KPI
- Trending Content: 실시간 트렌드

### 주요 메트릭
- Kafka Consumer Lag
- Spark Batch Processing Time
- API Quota Usage
- Error Rate

## 트러블슈팅

### Kafka Consumer Lag 발생
1. 파티션 수 증가
2. Spark Executor 수 증가
3. `maxOffsetsPerTrigger` 조정

### Spark OOM
1. Executor 메모리 증가
2. Shuffle 파티션 조정
3. Checkpoint 주기 조정

### API 쿼터 초과
1. 폴링 간격 조정
2. 쿼터 모니터링 확인
3. 요청 배치 크기 조정

## 라이선스

MIT License
