# Aggregator Service

실시간 토큰 거래 데이터 집계 서비스입니다.

## 개요

Aggregator Service는 Kafka에서 토큰 거래 데이터를 실시간으로 소비하여 다양한 시간 창(time window)에 대한 집계 데이터를 계산하고 Redis에 저장하는 서비스입니다.

## 주요 기능

- **실시간 거래 데이터 처리**: Kafka에서 거래 데이터를 실시간으로 소비
- **슬라이딩 윈도우 집계**: 1분, 5분, 15분, 30분, 1시간 단위의 집계 데이터 계산
- **Redis 저장**: 집계된 데이터를 Redis에 효율적으로 저장
- **성능 최적화**: 메모리 사용량, GC 튜닝, 동시성 최적화
- **모니터링**: 메트릭 수집 및 성능 모니터링
- **유지보수**: 자동 데이터 정리 및 복구 기능

## 아키텍처

```
Kafka → Consumer → Block Aggregator → Token Processors → Redis
                                   ↓
                              Metrics Collector
                                   ↓
                              Performance Monitor
```

## 설정

환경 변수를 통해 서비스를 설정할 수 있습니다. `.env.example` 파일을 참조하세요.

### 주요 설정

- **Kafka**: 브로커 주소, 토픽, 컨슈머 그룹
- **Redis**: 연결 정보, 풀 크기, 타임아웃
- **시간 창**: 집계할 시간 단위 설정
- **성능 튜닝**: GC 설정, 메모리 제한, 동시성 설정

## 실행

```bash
# 환경 변수 설정
cp .env.example .env
# .env 파일을 편집하여 설정 조정

# 서비스 빌드
go build -o aggregatorService

# 서비스 실행
./aggregatorService
```

## 모니터링

서비스는 HTTP 엔드포인트를 통해 모니터링할 수 있습니다:

- `GET /health` - 서비스 상태 확인
- `GET /health/ready` - 준비 상태 확인
- `GET /metrics` - Prometheus 형식 메트릭
- `GET /metrics/json` - JSON 형식 메트릭
- `GET /performance` - 성능 통계

## 성능 최적화

성능 최적화에 대한 자세한 내용은 [PERFORMANCE.md](PERFORMANCE.md)를 참조하세요.

### 성능 테스트

```bash
# 성능 테스트 스크립트 실행
./scripts/performance_test.sh
```

## 개발

### 테스트 실행

```bash
go test ./...
```

### 코드 구조

- `main.go` - 메인 서비스 진입점
- `config/` - 설정 관리
- `kafka/` - Kafka 컨슈머
- `processor/` - 거래 데이터 처리
- `aggregation/` - 슬라이딩 윈도우 집계
- `redis/` - Redis 연결 및 데이터 저장
- `metrics/` - 메트릭 수집 및 HTTP 서버
- `performance/` - 성능 튜닝 및 모니터링
- `maintenance/` - 유지보수 서비스
- `logging/` - 구조화된 로깅

## 라이센스

이 프로젝트는 MIT 라이센스 하에 배포됩니다.